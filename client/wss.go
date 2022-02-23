package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Wss constant
const (
	// Time allowed to write a message to the peer.
	writeWait = 30 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second
)

// Socket interface for easy test
type Socket interface {
	Send(*Message) error
	Recv() (*Message, error)
	Close() error
}

// WSocket Socket implementation for web socket
type WSocket struct {
	conn  *websocket.Conn
	mutex sync.RWMutex
}

func (ws *WSocket) getConn() *websocket.Conn {
	ws.mutex.RLock()
	defer ws.mutex.RUnlock()
	return ws.conn
}

// Close implements Socket.Close
func (ws *WSocket) Close() error {
	if conn := ws.getConn(); conn != nil {
		return conn.Close()
	}
	return nil
}

// Send implements Socket.Send
func (ws *WSocket) Send(msg *Message) error {
	conn := ws.getConn()
	if conn == nil {
		return fmt.Errorf("current connection is nil")
	}

	if err := conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}

	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	if err := conn.WriteJSON(msg); err != nil {
		return err
	}

	return nil
}

// Recv implements Socket.Recv
func (ws *WSocket) Recv() (*Message, error) {
	var msg *Message
	conn := ws.getConn()
	if conn == nil {
		return nil, fmt.Errorf("current connection is nil")
	}

	err := conn.ReadJSON(&msg)
	if err != nil {
		if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
			return nil, fmt.Errorf("websocket closeGoingAway error: %v", err)
		}
		return nil, fmt.Errorf("read wss err: %v", err)
	}

	return msg, nil
}
