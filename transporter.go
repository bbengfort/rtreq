package rtreq

import (
	"errors"
	"fmt"
	"os"

	pb "github.com/bbengfort/rtreq/msg"
	"github.com/gogo/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

//===========================================================================
// Network Transporter
//===========================================================================

// Transporter is a wrapper around a zmq.Socket object that is accessed by
// a single host, either remote or local. Both clients and servers are
// transporters.
//
// The primary role of the transporter is to send and receive messages
// defined as protocol buffers. They can wrap any type of ZMQ object and its
// up to the primary classes to instantiate the socket correctly.
type Transporter struct {
	name    string       // host information for the specified transporter
	addr    string       // address information of the connection
	context *zmq.Context // the zmq context to manage
	sock    *zmq.Socket  // the zmq socket to send and receive messages
	nSent   uint64       // number of messages sent
	nRecv   uint64       // number of messages received
	nBytes  uint64       // number of bytes sent
	stopped bool         // if the server is shutdown or not
}

// Init the transporter with the specified host and any other internal data.
func (t *Transporter) Init(addr, name string, context *zmq.Context) {
	t.addr = fmt.Sprintf("tcp://%s", addr)
	t.context = context

	// if name is empty string, set it to the hostname
	if name == "" {
		name, _ = os.Hostname()
	}
	t.name = name
}

// Close the socket and clean up the connections.
func (t *Transporter) Close() error {
	// Set linger to 0 so the connection closes immediately
	if err := t.sock.SetLinger(0); err != nil {
		return err
	}

	// Close the socket and return
	return t.sock.Close()
}

// Shutdown the ZMQ context permanently (should only be called once).
func (t *Transporter) Shutdown() error {
	t.stopped = true
	if err := t.context.Term(); err != nil {
		return err
	}

	if err := zmq.Term(); err != nil {
		return err
	}

	return nil
}

//===========================================================================
// Send and Recv Protobuf Messages
//===========================================================================

// Reads a zmq message from the socket and composes it into a protobuff
// message for handling downstream. This method blocks until a message is
// received.
func (t *Transporter) recv() (*pb.BasicMessage, error) {
	// Break if the socket hasn't been created
	if t.sock == nil {
		return nil, errors.New("socket is not initialized")
	}

	// Read the data off the wire
	bytes, err := t.sock.RecvBytes(0)
	if err != nil {
		return nil, err
	}

	// Parse the protocol buffers message
	message := new(pb.BasicMessage)
	if err := proto.Unmarshal(bytes, message); err != nil {
		return nil, err
	}

	// Increment the number of messages received
	t.nRecv++

	// Return the message
	return message, nil
}

// Composes a message into protocol buffers and puts it on the socket.
// Does not wait for the receiver, just fires off the reply.
func (t *Transporter) send(message string) error {
	if t.sock == nil {
		return errors.New("socket is not initialized")
	}

	// Compose the protobuf message
	msg := &pb.BasicMessage{
		Sender:  t.name,
		Message: message,
	}

	// Serialize the message
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Send the bytes on the wire
	nbytes, err := t.sock.SendBytes(data, zmq.DONTWAIT)
	if err != nil {
		return err
	}

	// Increment the number of messages sent and the number of bytes sent
	t.nBytes += uint64(nbytes)
	t.nSent++

	return nil
}
