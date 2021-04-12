package daemon

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/telepresenceio/telepresence/v2/pkg/iputil"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"

	"github.com/datawire/dlib/dlog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/client/daemon/dns"
)

const kubernetesZone = "cluster.local"

// outbound does stuff, idk, I didn't write it.
//
// A zero outbound is invalid; you must use newOutbound.
type outbound struct {
	dnsListener net.PacketConn
	dnsIP       net.IP
	noSearch    bool
	router      *tunRouter

	// Namespaces, accessible using <service-name>.<namespace-name>
	namespaces map[string]struct{}
	domains    map[string]iputil.IPs
	search     []string

	// managerConfigured is closed when the traffic manager has performed
	// its first update. The DNS resolver awaits this close and so does
	// the TUN device readers and writers
	managerConfigured      chan struct{}
	closeManagerConfigured sync.Once

	// dnsConfigured is closed when the dnsWorker has configured
	// the dnsServer.
	dnsConfigured chan struct{}

	kubeDNS     chan net.IP
	onceKubeDNS sync.Once

	// The domainsLock locks usage of namespaces, domains, and search
	domainsLock sync.RWMutex

	// Lock preventing concurrent calls to setSearchPath
	searchPathLock sync.Mutex

	setSearchPathFunc func(c context.Context, paths []string)

	work chan func(context.Context) error

	// dnsQueriesInProgress unique set of DNS queries currently in progress.
	dnsQueriesInProgress map[string]struct{}
	dnsQueriesLock       sync.Mutex
}

// splitToUDPAddr splits the given address into an UDPAddr. It's
// an  error if the address is based on a hostname rather than an IP.
func splitToUDPAddr(netAddr net.Addr) (*net.UDPAddr, error) {
	addr := netAddr.String()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	nsIP := net.ParseIP(host)
	if nsIP == nil {
		return nil, fmt.Errorf("host of address %q is not an IP address", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("port of address %s is not an integer", addr)
	}
	return &net.UDPAddr{IP: nsIP, Port: port}, nil
}

// newOutbound returns a new properly initialized outbound object.
//
// If dnsIP is empty, it will be detected from /etc/resolv.conf
func newOutbound(c context.Context, dnsIPStr string, noSearch bool) (*outbound, error) {
	lc := &net.ListenConfig{}
	listener, err := lc.ListenPacket(c, "udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	// seed random generator (used when shuffling IPs)
	rand.Seed(time.Now().UnixNano())

	ret := &outbound{
		dnsListener:          listener,
		dnsIP:                iputil.Parse(dnsIPStr),
		noSearch:             noSearch,
		namespaces:           make(map[string]struct{}),
		domains:              make(map[string]iputil.IPs),
		dnsQueriesInProgress: make(map[string]struct{}),
		search:               []string{""},
		work:                 make(chan func(context.Context) error),
		dnsConfigured:        make(chan struct{}),
		managerConfigured:    make(chan struct{}),
		kubeDNS:              make(chan net.IP, 1),
	}

	if ret.router, err = newTunRouter(ret.managerConfigured); err != nil {
		return nil, err
	}
	return ret, nil
}

// routerServerWorker starts the TUN router and reads from the work queue of firewall config
// changes that is written to by the 'Update' gRPC call.
func (o *outbound) routerServerWorker(c context.Context) (err error) {
	go func() {
		// No need to select between <-o.work and <-c.Done(); o.work will get closed when we start
		// shutting down.
		for f := range o.work {
			if c.Err() == nil {
				// As long as we're not shutting down, keep doing work.  (If we are shutting
				// down, do nothing but don't 'break'; keep draining the queue.)
				if err = f(c); err != nil {
					dlog.Error(c, err)
				}
			}
		}
	}()
	return o.router.run(c)
}

// On a MacOS, Docker uses its own search-path for single label names. This means that the search path that is declared
// in the MacOS resolver is ignored although the rest of the DNS-resolution works OK. Since the search-path is likely to
// change during a session, a stable fake domain is needed to emulate the search-path. That fake-domain can then be used
// in the search path declared in the Docker config. The "tel2-search" domain fills this purpose and a request for
// "<single label name>.tel2-search." will be resolved as "<single label name>." using the search path of this resolver.
const tel2SubDomain = "tel2-search"
const dotTel2SubDomain = "." + tel2SubDomain

func (o *outbound) resolveInCluster(c context.Context, query string) []net.IP {
	query = query[:len(query)-1]
	query = strings.ToLower(query) // strip of trailing dot
	query = strings.TrimSuffix(query, dotTel2SubDomain)

	o.dnsQueriesLock.Lock()
	for qip := range o.dnsQueriesInProgress {
		if strings.HasPrefix(query, qip) && strings.Contains(query, "."+kubernetesZone) {
			// This is most likely a recursion caused by the query in progress. This happens when a cluster
			// runs locally on the same host as Telepresence and falls back to use the DNS of that host when
			// the query cannot be resolved in the cluster. Sending that query to the traffic-manager is
			// pointless so we end the recursion here.
			o.dnsQueriesLock.Unlock()
			return nil
		}
	}
	o.dnsQueriesInProgress[query] = struct{}{}
	o.dnsQueriesLock.Unlock()

	defer func() {
		o.dnsQueriesLock.Lock()
		delete(o.dnsQueriesInProgress, query)
		o.dnsQueriesLock.Unlock()
	}()
	response, err := o.router.managerClient.LookupHost(c, &manager.LookupHostRequest{
		Session: o.router.session,
		Host:    query,
	})
	if err != nil {
		dlog.Error(c, err)
		return nil
	}
	if len(response.Ips) == 0 {
		return nil
	}
	ips := make(iputil.IPs, len(response.Ips))
	for i, ip := range response.Ips {
		ips[i] = ip
	}
	return ips
}

// Since headless and externalName services can have multiple IPs,
// we return a shuffled list of the IPs if there are more than one.
func shuffleIPs(ips iputil.IPs) iputil.IPs {
	switch lenIPs := len(ips); lenIPs {
	case 0:
		return iputil.IPs{}
	case 1:
	default:
		// If there are multiple elements in the slice, we shuffle the
		// order so it's not the same each time
		rand.Shuffle(lenIPs, func(i, j int) {
			ips[i], ips[j] = ips[j], ips[i]
		})
	}
	return ips
}

func (o *outbound) setManagerInfo(c context.Context, info *rpc.ManagerInfo) error {
	defer o.closeManagerConfigured.Do(func() {
		close(o.managerConfigured)
	})
	return o.router.setManagerInfo(c, info)
}

func (o *outbound) update(_ context.Context, table *rpc.Table) (err error) {
	// o.proxy.SetSocksPort(table.SocksPort)
	ips := make(map[IPKey]struct{}, len(table.Routes))
	domains := make(map[string]iputil.IPs)
	for _, route := range table.Routes {
		dIps := make(iputil.IPs, 0, len(route.Ips))
		for _, ipStr := range route.Ips {
			if ip := iputil.Parse(ipStr); ip != nil {
				dIps = append(dIps, ip)
				ips[IPKey(ip)] = struct{}{}
			}
		}
		if dn := route.Name; dn != "" {
			dn = strings.ToLower(dn) + "."
			domains[dn] = append(domains[dn], dIps...)
		}
	}
	o.work <- func(c context.Context) error {
		return o.doUpdate(c, domains, ips)
	}
	return nil
}

func (o *outbound) noMoreUpdates() {
	close(o.work)
}

func (o *outbound) doUpdate(c context.Context, domains map[string]iputil.IPs, table map[IPKey]struct{}) error {
	// We're updating routes. Make sure DNS waits until the new answer
	// is ready, i.e. don't serve old answers.
	o.domainsLock.Lock()
	defer o.domainsLock.Unlock()

	dnsChanged := false
	for k, ips := range o.domains {
		if _, ok := domains[k]; !ok {
			dnsChanged = true
			dlog.Debugf(c, "CLEAR %s -> %s", k, ips)
			delete(o.domains, k)
		}
	}

	var kubeDNS net.IP
	for k, nIps := range domains {
		// Ensure that all added entries contain unique, sorted, and non-empty IPs
		nIps = nIps.UniqueSorted()
		if len(nIps) == 0 {
			delete(domains, k)
			continue
		}
		domains[k] = nIps
		if oIps, ok := o.domains[k]; ok {
			if ok = len(nIps) == len(oIps); ok {
				for i, ip := range nIps {
					if ok = ip.Equal(oIps[i]); !ok {
						break
					}
				}
			}
			if !ok {
				dnsChanged = true
				dlog.Debugf(c, "REPLACE %s -> %s with %s", oIps, k, nIps)
			}
		} else {
			dnsChanged = true
			if k == "kube-dns.kube-system.svc.cluster.local." && len(nIps) >= 1 {
				kubeDNS = nIps[0]
			}
			dlog.Debugf(c, "STORE %s -> %s", k, nIps)
		}
	}

	// Operate on the copy of the current table and the new table
	ipsChanged := false
	oldIPs := o.router.snapshot()
	for ip := range table {
		if o.router.add(c, ip) {
			ipsChanged = true
		}
		delete(oldIPs, ip)
	}

	for ip := range oldIPs {
		if o.router.clear(c, ip) {
			ipsChanged = true
		}
	}

	if ipsChanged {
		if err := o.router.flush(c, kubeDNS); err != nil {
			dlog.Errorf(c, "flush: %v", err)
		}
	}

	if dnsChanged {
		o.domains = domains
		dns.Flush(c)
	}

	if kubeDNS != nil {
		o.onceKubeDNS.Do(func() { o.kubeDNS <- kubeDNS })
	}
	return nil
}

// SetSearchPath updates the DNS search path used by the resolver
func (o *outbound) setSearchPath(c context.Context, paths []string) {
	select {
	case <-c.Done():
	case <-o.dnsConfigured:
		o.searchPathLock.Lock()
		defer o.searchPathLock.Unlock()
		o.setSearchPathFunc(c, paths)
		dns.Flush(c)
	}
}
