package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lamhai1401/gologs/logs"
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
	Connect(_url string, args url.Values) error
	Send(*Message) error
	Recv() (*Message, error)
	Close() error
}

// WSocket Socket implementation for web socket
type WSocket struct {
	isClosed  bool
	originURL string
	conn      *websocket.Conn
	mutex     sync.RWMutex
}

func (ws *WSocket) checkClose() bool {
	ws.mutex.RLock()
	defer ws.mutex.RUnlock()
	return ws.isClosed
}

func (ws *WSocket) setClose(state bool) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	ws.isClosed = state
}

// Connect linter
func (ws *WSocket) Connect(_url string, args url.Values) error {
	surl, err := url.Parse(_url)

	if err != nil {
		return err
	}

	if !surl.IsAbs() {
		return errors.New("url should be absolute")
	}

	oscheme := surl.Scheme
	switch oscheme {
	case "ws":
		break
	case "wss":
		break
	case "http":
		surl.Scheme = "ws"
	case "https":
		surl.Scheme = "wss"
	default:
		return errors.New("schema should be http or https")
	}

	surl.Path = path.Join(surl.Path, "websocket")
	surl.RawQuery = args.Encode()
	originURL := fmt.Sprintf("%s://%s%s?%s", surl.Scheme, surl.Host, surl.Path, surl.RawQuery)
	ws.originURL = originURL

	return ws.dial(originURL)
}

func (ws *WSocket) getConn() *websocket.Conn {
	ws.mutex.RLock()
	defer ws.mutex.RUnlock()
	return ws.conn
}

func (ws *WSocket) setConn(conn *websocket.Conn) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	ws.conn = conn
}

// Close implements Socket.Close
func (ws *WSocket) Close() error {
	if conn := ws.getConn(); conn != nil {
		ws.setConn(nil)
		return conn.Close()
	}

	ws.setClose(true)
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

func (ws *WSocket) dial(_url string) error {
	var wsConn *websocket.Conn
	var err error
	dialer := websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  30 * time.Second,
		EnableCompression: true,
		ReadBufferSize:    maxMimumReadBuffer,
		WriteBufferSize:   maxMimumWriteBuffer,
	}

	limit := 100
	count := 0
	for {
		if count == limit {
			return fmt.Errorf("Try to connect phoenix 100 times and not connected, return")
		}
		count++
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(getTimeout())*time.Second)
		defer cancel()
		wsConn, _, err = dialer.DialContext(ctx, _url, nil)
		if err != nil {
			if isTimeoutError(err) {
				logs.Warn("*** Connection timeout. Try to reconnect")
				wsConn = nil
				err = nil
				ctx = nil
				time.Sleep(500 * time.Millisecond)
				continue
			} else {
				return err
			}
		}

		break
	}

	err = wsConn.SetCompressionLevel(6)
	if err != nil {
		return err
	}
	wsConn.EnableWriteCompression(true)
	ws.setConn(wsConn)
	return nil
}

func (ws *WSocket) reconnect() error {
	if conn := ws.getConn(); conn != nil {
		conn.Close()
		ws.setConn(nil)
	}

	return ws.dial(ws.originURL)
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
			if ws.checkClose() {
				return nil, fmt.Errorf("websocket closeGoingAway error: %v", err)
			}
			err := ws.reconnect()
			if err != nil {
				return nil, err
			}
			return ws.Recv()
		}
		return nil, fmt.Errorf("read wss err: %v", err)
	}

	return msg, nil
}

func getTimeout() int {
	i := 18
	if interval := os.Getenv("WSS_TIME_OUT"); interval != "" {
		j, err := strconv.Atoi(interval)
		if err == nil {
			i = j
		}
	}
	return i
}
