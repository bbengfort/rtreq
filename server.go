package rtreq

import (
	"fmt"

	pb "github.com/bbengfort/rtreq/msg"
	zmq "github.com/pebbe/zmq4"
)

//===========================================================================
// Server and HTTP Transport
//===========================================================================

// Server responds to requests from other peers.
type Server struct {
	Transporter
}

// Run the server and listen for messages
func (s *Server) Run() (err error) {

	// Create the socket
	if s.sock, err = s.context.NewSocket(zmq.REP); err != nil {
		return WrapError("could not create REP socket", err)
	}

	// Bind the socket and run the listener
	if err := s.sock.Bind(s.addr); err != nil {
		return WrapError("could not bind '%s'", err, s.addr)
	}
	status("bound to %s\n", s.addr)

	for {
		msg, err := s.recv()
		if err != nil {
			warne(err)
			break
		}
		s.handle(msg)
	}

	return s.Shutdown()
}

// Shutdown the server and clean up the socket
func (s *Server) Shutdown() error {
	info("shutting down")
	return s.Close()
}

//===========================================================================
// Message Handling
//===========================================================================

func (s *Server) handle(message *pb.BasicMessage) {
	info("received: %s\n", message.String())
	reply := fmt.Sprintf("reply msg #%d", s.nRecv)
	s.send(reply)
}
