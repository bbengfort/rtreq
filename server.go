package rtreq

import (
	zmq "github.com/pebbe/zmq4"
)

// NewServer creates a new rtreq.Server. If context is nil, it also creates
// a context that will be managed by the sever.
func NewServer(addr, name string, sync bool, nWorkers int, context *zmq.Context) (s Server, err error) {
	if context == nil {
		if context, err = zmq.NewContext(); err != nil {
			return nil, WrapError("could not create zmq context", err)
		}
	}

	if sync {
		s = new(RepServer)
		s.Init(addr, name, context)
		return s, nil
	}

	s = new(RouterServer)
	s.Init(addr, name, context)
	s.(*RouterServer).SetWorkers(nWorkers)
	return s, nil

}

//===========================================================================
// Server Interface
//===========================================================================

// Server represents a transporter that can respond to requests from peers.
type Server interface {
	Init(addr, name string, context *zmq.Context)
	Run() error
	Shutdown(path string) error
}
