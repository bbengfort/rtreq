package rtreq

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/bbengfort/rtreq/msg"
	zmq "github.com/pebbe/zmq4"
	"golang.org/x/sync/errgroup"
)

//===========================================================================
// Asynchronous Server Transporter
//===========================================================================

// IPCAddr is the in process communcation socket for workers.
const IPCAddr = "ipc://workers.ipc"

// DefaultNWorkers is the number of workers allocated to handle clients.
const DefaultNWorkers = 16

// RouterServer responds to requests from other peers using a ROUTER socket.
type RouterServer struct {
	sync.Mutex
	Transporter
	inproc   *zmq.Socket     // ipc DEALER socket to communicate with workers
	nWorkers int             // number of workers to initialize
	workers  *errgroup.Group // worker threads to handle requests
}

// Run the server and listen for messages
func (s *RouterServer) Run() (err error) {
	defer s.Close()
	defer s.Shutdown()

	// Create the socket to talk to clients
	if s.sock, err = s.context.NewSocket(zmq.ROUTER); err != nil {
		return WrapError("could not create ROUTER socket", err)
	}

	// Bind the client socket to the external address
	if err = s.sock.Bind(s.addr); err != nil {
		return WrapError("could not bind '%s'", err, s.addr)
	}
	status("bound async server to %s\n", s.addr)

	// Create the socket to talk to workers
	if s.inproc, err = s.context.NewSocket(zmq.DEALER); err != nil {
		return WrapError("could not create DEALER socket", err)
	}

	// Bind the workers socket to an inprocess address
	if err = s.inproc.Bind(IPCAddr); err != nil {
		return WrapError("could not bind '%s'", err, IPCAddr)
	}

	// Create the workers pool
	s.workers, _ = errgroup.WithContext(context.Background())
	for w := 0; w < s.nWorkers; w++ {
		worker := new(Worker)
		worker.Init(fmt.Sprintf("%s-%d", s.name, w+1), s.context)
		s.workers.Go(worker.Run)
	}
	info("initialized %d workers", s.nWorkers)

	// Connect worker threads to clients via a queue proxy
	if err = zmq.Proxy(s.sock, s.inproc, nil); err != nil {
		if !s.stopped {
			return WrapError("proxy interrupted", err)
		}

	}

	if !s.stopped {
		return s.workers.Wait()
	}

	return nil
}

// Close the socket and clean up the connections.
func (s *RouterServer) Close() (err error) {
	if err = s.inproc.Close(); err != nil {
		warn("could not close in process socket: %s", err)
	}

	// Set linger to 0 so the connection closes immediately
	if err = s.sock.SetLinger(0); err != nil {
		return err
	}

	// Close the socket and return
	return s.sock.Close()
}

// SetWorkers specifies the number of workers, if n is 0 uses DefaultNWorkers
func (s *RouterServer) SetWorkers(n int) {
	if n == 0 {
		n = DefaultNWorkers
	}
	s.nWorkers = n
}

//===========================================================================
// Message Handling Workers
//===========================================================================

// Worker connects to an inprocess socket and handle client messages in
// parallel without sharing state. Workers have all the benefits of other
// transporters, but maintain local sockets.
type Worker struct {
	Transporter
}

// Init the worker and connect it.
func (w *Worker) Init(name string, context *zmq.Context) {
	w.addr = IPCAddr
	w.context = context
	w.name = name

	var err error
	w.sock, err = context.NewSocket(zmq.REP)
	if err != nil {
		w.sock = nil
		warn("could not create worker REP socket: %s", err)
		return
	}

	if err = w.sock.Connect(w.addr); err != nil {
		w.sock = nil
		warn("could not connect to '%s': %s", w.addr, err)
		return
	}
}

// Run the worker to listen for messages and respond to them.
func (w *Worker) Run() error {
	debug("starting worker %s", w.name)

	// Handle messages received on the receiver forever
	for {
		msg, err := w.recv()
		if err != nil {
			debug("error in %s: %s", w.name, err)
			break
		}
		w.handle(msg)
	}

	return w.Close()
}

// Handle messages received by the worker
func (w *Worker) handle(message *pb.BasicMessage) {
	info("received: %s\n", message.String())
	reply := fmt.Sprintf("reply msg #%d from worker %s", w.nRecv, w.name)
	w.send(reply)
}
