package chanserver

import (
	"bufio"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
)

// ChanServer listens for requests and sends them on a channel.
type ChanServer struct {
	Addr net.Addr
	Reqs <-chan *http.Request

	statusCode int32
	l          net.Listener
	r          chan *http.Request
	quit       chan struct{}
	wg         sync.WaitGroup
}

// New starts a ChanServer listening on a random address.
func New() (*ChanServer, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	ch := &ChanServer{
		l:          l,
		statusCode: 200,
		r:          make(chan *http.Request, 1),
		quit:       make(chan struct{}),
	}
	ch.wg.Add(1)
	go ch.listen()
	ch.Addr = l.Addr()
	ch.Reqs = ch.r
	return ch, nil
}

func (ch *ChanServer) listen() {
	defer ch.wg.Done()
	for {
		select {
		case <-ch.quit:
			return
		default:
		}
		conn, err := ch.l.Accept()
		if err != nil {
			continue
		}
		_ = ch.handle(conn)
	}
}

func (ch *ChanServer) handle(conn net.Conn) error {
	defer func() {
		_ = conn.Close()
	}()
	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		return err
	}
	ch.r <- req
	resp := &http.Response{
		StatusCode: int(atomic.LoadInt32(&ch.statusCode)),
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	return resp.Write(conn)
}

// SetStatusCode changes the HTTP status code in the server's response.
func (ch *ChanServer) SetStatusCode(status int) {
	atomic.StoreInt32(&ch.statusCode, int32(status))
}

// Close the server and channel.
func (ch *ChanServer) Close() error {
	err := ch.l.Close()
	ch.quit <- struct{}{}
	ch.wg.Wait()
	close(ch.r)
	close(ch.quit)
	return err
}
