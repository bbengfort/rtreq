package rtreq

import (
	"fmt"

	pb "github.com/bbengfort/rtreq/msg"
	zmq "github.com/pebbe/zmq4"
)

//===========================================================================
// Synchronous Server Transporter
//===========================================================================

// RepServer responds to requests from other peers using a REP socket.
type RepServer struct {
	Transporter
}

// Run the server and listen for messages
func (s *RepServer) Run() (err error) {

	// Create the socket
	if s.sock, err = s.context.NewSocket(zmq.REP); err != nil {
		return WrapError("could not create REP socket", err)
	}

	// Bind the socket and run the listener
	if err := s.sock.Bind(s.addr); err != nil {
		return WrapError("could not bind '%s'", err, s.addr)
	}
	status("bound sync server to %s\n", s.addr)

	for {
		msg, err := s.recv()
		if err != nil {
			warne(err)
			break
		}
		s.handle(msg)
	}

	if err := s.Close(); err != nil {
		return err
	}

	return s.Shutdown()
}

//===========================================================================
// Message Handling
//===========================================================================

func (s *RepServer) handle(message *pb.BasicMessage) {
	info("received: %s\n", message.String())
	reply := fmt.Sprintf("reply msg #%d", s.nRecv)
	s.send(reply)
}
