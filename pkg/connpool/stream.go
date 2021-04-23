package connpool

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/datawire/dlib/dlog"
)

type Stream struct {
	ConnTunnelStream
	pool *Pool
	rnd  *rand.Rand
}

func NewStream(bidiStream ConnTunnelStream, pool *Pool) *Stream {
	return &Stream{ConnTunnelStream: bidiStream, pool: pool, rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (s *Stream) RandomSequence() int32 {
	return s.rnd.Int31()
}

// ReadLoop reads replies from the stream and dispatches them to the correct connection
// based on the message id.
func (s *Stream) ReadLoop(ctx context.Context, closing *int32) error {
	for atomic.LoadInt32(closing) == 0 {
		cm, err := s.Recv()
		if err != nil {
			if atomic.LoadInt32(closing) == 0 && ctx.Err() == nil {
				return fmt.Errorf("read from grpc.ClientStream failed: %s", err)
			}
			return nil
		}

		if IsControlMessage(cm) {
			ctrl, err := NewControlMessage(cm)
			if err != nil {
				dlog.Error(ctx, err)
			} else {
				s.handleControl(ctx, ctrl)
			}
		} else {
			id := ConnID(cm.ConnId)
			dlog.Debugf(ctx, "<- MGR %s, len %d", id.ReplyString(), len(cm.Payload))
			if conn, _, _ := s.pool.Get(ctx, id, nil); conn != nil {
				conn.HandleMessage(ctx, cm)
			}
		}
	}
	return nil
}

func (s *Stream) handleControl(ctx context.Context, ctrl *ControlMessage) {
	id := ctrl.ID

	dlog.Debugf(ctx, "<- MGR %s, code %s", id.ReplyString(), ctrl.Code)
	conn, _, err := s.pool.Get(ctx, id, func(ctx context.Context, release func()) (Handler, error) {
		if ctrl.Code != Connect {
			// Only Connect requested from peer may create a new instance at this point
			return nil, nil
		}
		return NewDialer(id, s.ConnTunnelStream, release), nil
	})
	if err != nil {
		dlog.Error(ctx, err)
		return
	}
	if conn != nil {
		conn.HandleControl(ctx, ctrl)
		return
	}
	if ctrl.Code != ReadClosed && ctrl.Code != DisconnectOK {
		dlog.Error(ctx, "control packet lost because no connection was active")
	}
}
