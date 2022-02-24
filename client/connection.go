package client

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/lamhai1401/gologs/logs"
)

const maxMimumReadBuffer = 1024 * 1024 * 2
const maxMimumWriteBuffer = 1024 * 1024 * 2

// VSN linter
const VSN = "1.0.0"

const (
	// ConnConnecting linter
	ConnConnecting = "connecting"
	// ConnOpen linter
	ConnOpen = "open"
	// ConnClosing linter
	ConnClosing = "closing"
	// Connclosed linter
	Connclosed = "closed"
)

// Connection handle phoenix connection
type Connection struct {
	sock   Socket
	ref    refMaker
	center *regCenter

	recvMsg chan *Message
	msgs    chan *Message // for sending msg
	status  string
	mutex   sync.RWMutex
	ctx     context.Context
	cancel  func()
}

// Connect need  to call first, auto reconnect if failed
func Connect(_url string, args url.Values) (*Connection, error) {
	wsConn := &WSocket{}
	err := wsConn.Connect(_url, args)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		ctx:     ctx,
		cancel:  cancel,
		sock:    wsConn,
		center:  newRegCenter(),
		msgs:    make(chan *Message, connMsgSize),
		recvMsg: make(chan *Message, connMsgSize),
		status:  ConnOpen,
	}

	conn.start()

	conn.status = ConnOpen
	return conn, nil
}

func isTimeoutError(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}

// Close linter
func (conn *Connection) Close() {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	conn.status = Connclosed
	conn.sock.Close()
	conn.cancel()
}

func (conn *Connection) getSock() Socket {
	conn.mutex.RLock()
	defer conn.mutex.RUnlock()
	return conn.sock
}

// OnMessage receive all message on connection
func (conn *Connection) OnMessage() *Puller {
	return conn.center.register(all)
}

func (conn *Connection) push(msg *Message) error {
	sock := conn.getSock()
	if sock == nil {
		return fmt.Errorf("Socket is nil")
	}
	return sock.Send(msg)
}

func (conn *Connection) getCtx() context.Context {
	conn.mutex.RLock()
	defer conn.mutex.RUnlock()
	return conn.ctx
}

func (conn *Connection) heartbeatLoop() {
	msg := &Message{
		Topic:   "phoenix",
		Event:   "heartbeat",
		Payload: "",
	}

	ticker := time.NewTicker(30 * time.Second)
	ctx := conn.getCtx()
	if ctx == nil {
		logs.Warn("heartbeatLoop cannot start, connection ctx is nil")
		return
	}
	for {
		select {
		case <-ticker.C:
			// Set the message reference right before we send it:
			msg.Ref = conn.ref.makeRef()
			conn.sock.Send(msg)
		case <-ctx.Done():
			return
		}
	}
}

func (conn *Connection) start() {
	go conn.pullLoop()
	go conn.heartbeatLoop()
	go conn.readLoop()
	go conn.coreLoop()
}

func (conn *Connection) readLoop() {
	ctx := conn.getCtx()
	if ctx == nil {
		logs.Warn("heartbeatLoop cannot start, connection ctx is nil")
		return
	}

	for {
		if conn.status == Connclosed {
			return
		}

		sock := conn.getSock()
		if sock == nil {
			logs.Warn("Socket is nil")
			continue
		}
		msg, err := sock.Recv()
		if err != nil {
			conn.msgs <- nil
			logs.Error(fmt.Sprintf("%s\n", err))
			// close(conn.msgs)       // TODO maybe leak here
			conn.closeAllPullers()
			return
		}
		conn.recvMsg <- msg
	}
}

func (conn *Connection) pullLoop() {
	ctx := conn.getCtx()
	if ctx == nil {
		logs.Warn("heartbeatLoop cannot start, connection ctx is nil")
		return
	}
	for {
		select {
		case msg := <-conn.recvMsg:
			if conn.status == Connclosed {
				return
			}
			conn.msgs <- msg
		case <-ctx.Done():
			return
		}
	}
}

func (conn *Connection) closePullers(pullers []*Puller) {
	for _, puller := range pullers {
		close(puller.ch)
	}
}

func (conn *Connection) closeAllPullers() {
	conn.center.RLock()
	for id, pullers := range conn.center.regs {
		delete(conn.center.regs, id)
		defer func(pls []*Puller) {
			time.Sleep(2 * time.Second)
			conn.closePullers(pls)
		}(pullers)
	}
	conn.center.RUnlock()

}

func (conn *Connection) coreLoop() {
	ctx := conn.getCtx()
	if ctx == nil {
		logs.Warn("coreLoop cannot start, connection ctx is nil")
		return
	}
	for {
		select {
		case msg, open := <-conn.msgs:
			if !open || msg == nil {
				return
			}
			conn.dispatch(msg)
		case <-ctx.Done():
			return
		}
	}
}

func (conn *Connection) dispatch(msg *Message) {
	var wg sync.WaitGroup
	wg.Add(4)
	go conn.pushToChans(&wg, conn.center.getPullers(all), msg)
	go conn.pushToChans(&wg, conn.center.getPullers(toKey(msg.Topic, "", "")), msg)
	go conn.pushToChans(&wg, conn.center.getPullers(toKey(msg.Topic, msg.Event, "")), msg)
	go conn.pushToChans(&wg, conn.center.getPullers(toKey("", "", msg.Ref)), msg)
	wg.Wait()
}

func (conn *Connection) pushToChans(wg *sync.WaitGroup, pullers []*Puller, msg *Message) {
	defer wg.Done()
	if len(pullers) == 0 {
		return
	}
	for _, puller := range pullers {
		go conn.pushToChan(puller, msg)
	}
}

func (conn *Connection) pushToChan(puller *Puller, msg *Message) {
	select {
	case puller.ch <- msg:
	case <-time.After(10 * time.Millisecond):
	}
}
