package manager

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/telepresenceio/telepresence/v2/pkg/iputil"

	"golang.org/x/sys/unix"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	empty "google.golang.org/protobuf/types/known/emptypb"

	"github.com/datawire/dlib/dlog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/rpc/v2/systema"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/internal/state"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/internal/tcpgrpc"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/internal/udpgrpc"
	"github.com/telepresenceio/telepresence/v2/pkg/connpool"
	"github.com/telepresenceio/telepresence/v2/pkg/version"
)

// Clock is the mechanism used by the Manager state to get the current time.
type Clock interface {
	Now() time.Time
}

type Manager struct {
	ctx     context.Context
	clock   Clock
	env     Env
	ID      string
	state   *state.State
	systema *systemaPool

	rpc.UnsafeManagerServer
}

var _ rpc.ManagerServer = &Manager{}

type wall struct{}

func (wall) Now() time.Time {
	return time.Now()
}

func NewManager(ctx context.Context, env Env) *Manager {
	ret := &Manager{
		ctx:   ctx,
		clock: wall{},
		env:   env,
		ID:    uuid.New().String(),
		state: state.NewState(ctx),
	}
	ret.systema = NewSystemAPool(ret)
	return ret
}

// Version returns the version information of the Manager.
func (*Manager) Version(context.Context, *empty.Empty) (*rpc.VersionInfo2, error) {
	return &rpc.VersionInfo2{Version: version.Version}, nil
}

// ArriveAsClient establishes a session between a client and the Manager.
func (m *Manager) ArriveAsClient(ctx context.Context, client *rpc.ClientInfo) (*rpc.SessionInfo, error) {
	dlog.Debug(ctx, "ArriveAsClient called")

	if val := validateClient(client); val != "" {
		return nil, status.Errorf(codes.InvalidArgument, val)
	}

	sessionID := m.state.AddClient(client, m.clock.Now())

	return &rpc.SessionInfo{
		SessionId: sessionID,
	}, nil
}

// ArriveAsAgent establishes a session between an agent and the Manager.
func (m *Manager) ArriveAsAgent(ctx context.Context, agent *rpc.AgentInfo) (*rpc.SessionInfo, error) {
	dlog.Debug(ctx, "ArriveAsAgent called")

	if val := validateAgent(agent); val != "" {
		return nil, status.Errorf(codes.InvalidArgument, val)
	}

	sessionID := m.state.AddAgent(agent, m.clock.Now())

	return &rpc.SessionInfo{SessionId: sessionID}, nil
}

// Remain indicates that the session is still valid.
func (m *Manager) Remain(ctx context.Context, req *rpc.RemainRequest) (*empty.Empty, error) {
	ctx = WithSessionInfo(ctx, req.GetSession())
	dlog.Debugf(ctx, "Remain called")

	if ok := m.state.MarkSession(req, m.clock.Now()); !ok {
		return nil, status.Errorf(codes.NotFound, "Session %q not found", req.GetSession().GetSessionId())
	}

	return &empty.Empty{}, nil
}

// Depart terminates a session.
func (m *Manager) Depart(ctx context.Context, session *rpc.SessionInfo) (*empty.Empty, error) {
	ctx = WithSessionInfo(ctx, session)
	dlog.Debugf(ctx, "Depart called")

	m.state.RemoveSession(session.GetSessionId())

	return &empty.Empty{}, nil
}

// WatchAgents notifies a client of the set of known Agents.
func (m *Manager) WatchAgents(session *rpc.SessionInfo, stream rpc.Manager_WatchAgentsServer) error {
	ctx := WithSessionInfo(stream.Context(), session)

	dlog.Debugf(ctx, "WatchAgents called")

	snapshotCh := m.state.WatchAgents(ctx, nil)
	for {
		select {
		case snapshot, ok := <-snapshotCh:
			if !ok {
				// The request has been canceled.
				return nil
			}
			agents := make([]*rpc.AgentInfo, 0, len(snapshot.State))
			for _, agent := range snapshot.State {
				agents = append(agents, agent)
			}
			resp := &rpc.AgentInfoSnapshot{
				Agents: agents,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		case <-m.state.SessionDone(session.GetSessionId()):
			// Manager believes this session has ended.
			return nil
		}
	}
}

// WatchIntercepts notifies a client or agent of the set of intercepts
// relevant to that client or agent.
func (m *Manager) WatchIntercepts(session *rpc.SessionInfo, stream rpc.Manager_WatchInterceptsServer) error {
	ctx := WithSessionInfo(stream.Context(), session)
	sessionID := session.GetSessionId()

	dlog.Debugf(ctx, "WatchIntercepts called")

	var filter func(id string, info *rpc.InterceptInfo) bool
	if sessionID == "" {
		// No sessonID; watch everything
		filter = func(id string, info *rpc.InterceptInfo) bool {
			return true
		}
	} else if agent := m.state.GetAgent(sessionID); agent != nil {
		// sessionID refers to an agent session
		filter = func(id string, info *rpc.InterceptInfo) bool {
			// Don't return intercepts for different agents.
			if info.Spec.Namespace != agent.Namespace || info.Spec.Agent != agent.Name {
				return false
			}
			// Don't return intercepts that aren't in a "agent-owned" state.
			switch info.Disposition {
			case rpc.InterceptDispositionType_WAITING:
			case rpc.InterceptDispositionType_ACTIVE:
			case rpc.InterceptDispositionType_AGENT_ERROR:
				// agent-owned state: continue along
			default:
				// otherwise: don't return this intercept
				return false
			}
			// We haven't found a reason to exlude this intercept, so include it.
			return true
		}
	} else {
		// sessionID refers to a client session
		filter = func(id string, info *rpc.InterceptInfo) bool {
			return info.ClientSession.SessionId == sessionID
		}
	}

	var sessionDone <-chan struct{}
	if sessionID == "" {
		ch := make(chan struct{})
		defer close(ch)
		sessionDone = ch
	} else {
		sessionDone = m.state.SessionDone(sessionID)
	}

	snapshotCh := m.state.WatchIntercepts(ctx, filter)
	for {
		select {
		case snapshot, ok := <-snapshotCh:
			if !ok {
				dlog.Debugf(ctx, "WatchIntercepts request cancelled")
				return nil
			}
			dlog.Debugf(ctx, "WatchIntercepts sending update")
			intercepts := make([]*rpc.InterceptInfo, 0, len(snapshot.State))
			for _, intercept := range snapshot.State {
				intercepts = append(intercepts, intercept)
			}
			resp := &rpc.InterceptInfoSnapshot{
				Intercepts: intercepts,
			}
			sort.Slice(intercepts, func(i, j int) bool {
				return intercepts[i].Id < intercepts[j].Id
			})
			if err := stream.Send(resp); err != nil {
				dlog.Debugf(ctx, "WatchIntercepts encountered a write error: %v", err)
				return err
			}
		case <-sessionDone:
			dlog.Debugf(ctx, "WatchIntercepts session cancelled")
			return nil
		}
	}
}

// CreateIntercept lets a client create an intercept.
func (m *Manager) CreateIntercept(ctx context.Context, ciReq *rpc.CreateInterceptRequest) (*rpc.InterceptInfo, error) {
	ctx = WithSessionInfo(ctx, ciReq.GetSession())
	sessionID := ciReq.GetSession().GetSessionId()
	spec := ciReq.InterceptSpec

	dlog.Debugf(ctx, "CreateIntercept called")

	if m.state.GetClient(sessionID) == nil {
		return nil, status.Errorf(codes.NotFound, "Client session %q not found", sessionID)
	}

	if val := validateIntercept(spec); val != "" {
		return nil, status.Errorf(codes.InvalidArgument, val)
	}

	return m.state.AddIntercept(sessionID, spec)
}

func (m *Manager) UpdateIntercept(ctx context.Context, req *rpc.UpdateInterceptRequest) (*rpc.InterceptInfo, error) {
	ctx = WithSessionInfo(ctx, req.GetSession())
	sessionID := req.GetSession().GetSessionId()
	var interceptID string
	// When something without a session ID (e.g. System A) calls this function,
	// it is sending the intercept ID as the name, so we use that.
	//
	// TODO: Look at cmd/traffic/cmd/manager/internal/state API and see if it makes
	// sense to make more / all functions use intercept ID instead of session ID + name.
	// Or at least functions outside services (e.g. SystemA), which don't know about sessions,
	// use in requests.
	if sessionID == "" {
		interceptID = req.Name
	} else {
		if m.state.GetClient(sessionID) == nil {
			return nil, status.Errorf(codes.NotFound, "Client session %q not found", sessionID)
		}
		interceptID = sessionID + ":" + req.Name
	}

	dlog.Debugf(ctx, "UpdateIntercept called: %s", interceptID)

	switch action := req.PreviewDomainAction.(type) {
	case *rpc.UpdateInterceptRequest_AddPreviewDomain:
		var domain string
		var sa systema.SystemACRUDClient
		var err error
		intercept := m.state.UpdateIntercept(interceptID, func(intercept *rpc.InterceptInfo) {
			// Check if this is already done.
			if intercept.PreviewDomain != "" {
				return
			}

			// Connect to SystemA.
			if sa == nil {
				sa, err = m.systema.Get()
				if err != nil {
					err = errors.Wrap(err, "systema: acquire connection")
					return
				}
			}

			// Have SystemA create the preview domain.
			if domain == "" {
				var resp *systema.CreateDomainResponse
				resp, err = sa.CreateDomain(ctx, &systema.CreateDomainRequest{
					InterceptId:   intercept.Id,
					DisplayBanner: action.AddPreviewDomain.DisplayBanner,
					InterceptSpec: intercept.Spec,
					Host:          action.AddPreviewDomain.Ingress.L5Host,
				})
				if err != nil {
					err = errors.Wrap(err, "systema: create domain")
					return
				}
				domain = resp.Domain
			}

			// Apply that to the intercept.
			intercept.PreviewDomain = domain
			intercept.PreviewSpec = action.AddPreviewDomain
		})
		if err != nil || intercept == nil || domain == "" || intercept.PreviewDomain != domain {
			// Oh no, something went wrong.  Clean up.
			if sa != nil {
				if domain != "" {
					_, err := sa.RemoveDomain(ctx, &systema.RemoveDomainRequest{
						Domain: domain,
					})
					if err != nil {
						dlog.Errorln(ctx, "systema: remove domain:", err)
					}
				}
				if err := m.systema.Done(); err != nil {
					dlog.Errorln(ctx, "systema: release connection:", err)
				}
			}
		}
		if intercept == nil {
			err = status.Errorf(codes.NotFound, "Intercept with ID %q not found for this session", interceptID)
		}
		return intercept, err
	case *rpc.UpdateInterceptRequest_RemovePreviewDomain:
		var domain string
		intercept := m.state.UpdateIntercept(interceptID, func(intercept *rpc.InterceptInfo) {
			// Check if this is already done.
			if intercept.PreviewDomain == "" {
				return
			}

			// Remove the domain
			domain = intercept.PreviewDomain
			intercept.PreviewDomain = ""
		})
		if domain != "" {
			if sa, err := m.systema.Get(); err != nil {
				dlog.Errorln(ctx, "systema: acquire connection:", err)
			} else {
				_, err := sa.RemoveDomain(ctx, &systema.RemoveDomainRequest{
					Domain: domain,
				})
				if err != nil {
					dlog.Errorln(ctx, "systema: remove domain:", err)
				}
				if err := m.systema.Done(); err != nil {
					dlog.Errorln(ctx, "systema: release connection:", err)
				}
			}
		}
		if intercept == nil {
			return nil, status.Errorf(codes.NotFound, "Intercept with ID %q not found for this session", interceptID)
		}
		return intercept, nil
	default:
		panic(errors.Errorf("Unimplemented UpdateInterceptRequest action: %T", action))
	}
}

// RemoveIntercept lets a client remove an intercept.
func (m *Manager) RemoveIntercept(ctx context.Context, riReq *rpc.RemoveInterceptRequest2) (*empty.Empty, error) {
	ctx = WithSessionInfo(ctx, riReq.GetSession())
	sessionID := riReq.GetSession().GetSessionId()
	name := riReq.Name

	dlog.Debugf(ctx, "RemoveIntercept called: %s", name)

	if m.state.GetClient(sessionID) == nil {
		return nil, status.Errorf(codes.NotFound, "Client session %q not found", sessionID)
	}

	if !m.state.RemoveIntercept(sessionID, name) {
		return nil, status.Errorf(codes.NotFound, "Intercept named %q not found", name)
	}

	return &empty.Empty{}, nil
}

// ReviewIntercept lets an agent approve or reject an intercept.
func (m *Manager) ReviewIntercept(ctx context.Context, rIReq *rpc.ReviewInterceptRequest) (*empty.Empty, error) {
	ctx = WithSessionInfo(ctx, rIReq.GetSession())
	sessionID := rIReq.GetSession().GetSessionId()
	ceptID := rIReq.Id

	dlog.Debugf(ctx, "ReviewIntercept called: %s - %s", ceptID, rIReq.Disposition)

	agent := m.state.GetAgent(sessionID)
	if agent == nil {
		return nil, status.Errorf(codes.NotFound, "Agent session %q not found", sessionID)
	}

	intercept := m.state.UpdateIntercept(ceptID, func(intercept *rpc.InterceptInfo) {
		// Sanity check: The reviewing agent must be an agent for the intercept.
		if intercept.Spec.Namespace != agent.Namespace || intercept.Spec.Agent != agent.Name {
			return
		}

		// Only update intercepts in the waiting state.  Agents race to review an intercept, but we
		// expect they will always compatible answers.
		if intercept.Disposition == rpc.InterceptDispositionType_WAITING {
			intercept.Disposition = rIReq.Disposition
			intercept.Message = rIReq.Message
			intercept.PodName = rIReq.PodName
			intercept.SshPort = rIReq.SshPort
			intercept.MechanismArgsDesc = rIReq.MechanismArgsDesc
		}
	})

	if intercept == nil {
		return nil, status.Errorf(codes.NotFound, "Intercept with ID %q not found for this session", ceptID)
	}

	return &empty.Empty{}, nil
}

func (m *Manager) ConnTunnel(server rpc.Manager_ConnTunnelServer) error {
	dlog.Debug(m.ctx, "Established TCP tunnel")
	pool := connpool.NewPool() // must have one pool per tunnel (per client, really)
	for {
		dg, err := server.Recv()
		if err != nil {
			if m.ctx.Err() != nil {
				err = nil
			}
			return err
		}
		m.handleTunnelMessage(pool, server, dg)
	}
}

func (m *Manager) LookupHost(ctx context.Context, request *rpc.LookupHostRequest) (*rpc.LookupHostResponse, error) {
	ctx = WithSessionInfo(ctx, request.GetSession())
	dlog.Debugf(ctx, "LookupHost called %s", request.Host)
	sessionID := request.GetSession().GetSessionId()
	client := m.state.GetClient(sessionID)

	iceptAgentIDs := m.state.GetAgentsInterceptedByClient(client.Name)
	iceptCount := len(iceptAgentIDs)

	ips := iputil.IPs{}
	if iceptCount > 0 {
		rsMu := sync.Mutex{} // prevent concurrent updates of result.Ips slice
		wg := sync.WaitGroup{}
		wg.Add(iceptCount)
		agentTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		responseCount := 0
		defer cancel()
		for _, agentSessionID := range m.state.GetAgentsInterceptedByClient(client.Name) {
			go func(agentSessionID string) {
				defer func() {
					m.state.EndHostLookup(agentSessionID, request)
					wg.Done()
				}()

				rsCh := m.state.StartHostLookup(agentSessionID, request)
				if rsCh == nil {
					return
				}
				select {
				case <-agentTimeout.Done():
					return
				case rs := <-rsCh:
					rsMu.Lock()
					responseCount++
					rc := responseCount
					for _, ip := range rs.Ips {
						ips = append(ips, ip)
					}
					rsMu.Unlock()
					if rc == iceptCount {
						// all agents have responded
						return
					}
				}
			}(agentSessionID)
		}
		wg.Wait() // wait for timeout or that all agents have responded

		result := &rpc.LookupHostResponse{}
		if len(ips) > 0 {
			ips = ips.UniqueSorted()
			dlog.Debugf(ctx, "LookupHost response from agents %s -> %s", request.Host, ips)
			result.Ips = ips.BytesSlice()
			return result, nil
		}
	}

	// Either we aren't intercepting any agents, or none of them was able to find the given host. Let's
	// try from the manager too.
	addrs, err := net.LookupHost(request.Host)
	response := &rpc.LookupHostResponse{}
	if err != nil {
		response.Ips = [][]byte{}
		dlog.Debugf(ctx, "LookupHost response %s -> NOT FOUND", request.Host)
		return response, nil
	}
	ips = make(iputil.IPs, len(addrs))
	for i, addr := range addrs {
		ips[i] = iputil.Parse(addr)
	}
	dlog.Debugf(ctx, "LookupHost response from agents %s -> %s", request.Host, ips)
	response.Ips = ips.BytesSlice()
	return response, nil
}

func (m *Manager) AgentLookupHostResponse(ctx context.Context, response *rpc.LookupHostAgentResponse) (*empty.Empty, error) {
	ctx = WithSessionInfo(ctx, response.GetSession())
	dlog.Debugf(ctx, "AgentLookupHostResponse called %s -> %s", response.Request.Host, iputil.IPsFromBytesSlice(response.Response.Ips))
	m.state.PostLookupResponse(response)
	return &empty.Empty{}, nil
}

func (m *Manager) WatchLookupHost(session *rpc.SessionInfo, stream rpc.Manager_WatchLookupHostServer) error {
	ctx := WithSessionInfo(stream.Context(), session)
	dlog.Debugf(ctx, "WatchLookupHost called")
	lrCh := m.state.WatchLookupHost(session.SessionId)
	for {
		select {
		case <-m.ctx.Done():
			return nil
		case lr := <-lrCh:
			if err := stream.Send(lr); err != nil {
				return err
			}
		}
	}
}

func (m *Manager) handleTunnelMessage(pool *connpool.Pool, server rpc.Manager_ConnTunnelServer, cm *rpc.ConnMessage) {
	var id connpool.ConnID
	var ctrl *connpool.ControlMessage
	var err error
	if connpool.IsControlMessage(cm) {
		ctrl, err = connpool.NewControlMessage(cm)
		if err != nil {
			dlog.Error(m.ctx, err)
			return
		}
		id = ctrl.ID
	} else {
		id = connpool.ConnID(cm.ConnId)
	}

	// Retrieve the connection that is tracked for the given id. Create a new one if necessary
	h, err := pool.Get(m.ctx, id, func(ctx context.Context, release func()) (connpool.Handler, error) {
		switch id.Protocol() {
		case unix.IPPROTO_TCP:
			return tcpgrpc.NewHandler(ctx, id, server, release)
		case unix.IPPROTO_UDP:
			return udpgrpc.NewHandler(ctx, id, server, release)
		default:
			return nil, fmt.Errorf("unhadled L4 protocol: %d", id.Protocol())
		}
	})
	if err != nil {
		dlog.Errorf(m.ctx, "failed to get connection handler: %v", err)
		return
	}
	if ctrl != nil {
		h.HandleControl(m.ctx, ctrl)
	} else {
		h.HandleMessage(m.ctx, cm)
	}
}

// expire removes stale sessions.
func (m *Manager) expire() {
	m.state.ExpireSessions(m.clock.Now().Add(-15 * time.Second))
}
