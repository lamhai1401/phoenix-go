package client

import "fmt"

const (
	// Channel status

	// ChClosed channel closed
	ChClosed = "closed"
	// ChErrored channel errored
	ChErrored = "errored"
	// ChJoined channel joined
	ChJoined = "joined"
	// ChJoing channel joining
	ChJoing = "joining"

	// Channel events

	// ChanClose channel close event
	ChanClose = "phx_close"
	// ChanError channel error event
	ChanError = "phx_error"
	// ChanReply server reply event
	ChanReply = "phx_reply"
	// ChanJoin  client send join event
	ChanJoin = "phx_join"
	// ChanLeave client send leave event
	ChanLeave = "phx_leave"
)

// Chan channel
type Chan struct {
	conn   *Connection
	topic  string
	status string
}

// Chan create a new channel on connection
func (conn *Connection) Chan(topic string) (*Chan, error) {
	if conn.status != ConnOpen {
		return nil, fmt.Errorf("Connection is " + conn.status)
	}

	ch := &Chan{
		conn:   conn,
		topic:  topic,
		status: ChJoing,
	}

	return ch, nil
}

// Push send a msg to channel
func (ch *Chan) Push(evt string, payload interface{}) error {
	msg := &Message{
		Topic:   ch.topic,
		Event:   evt,
		Ref:     ch.conn.ref.makeRef(),
		Payload: payload,
	}
	return ch.conn.push(msg)
}

// PushWithRef send a msg to channel
func (ch *Chan) PushWithRef(ref, evt *string, payload interface{}) error {
	msg := &Message{
		Topic:   ch.topic,
		Event:   *evt,
		Ref:     *ref,
		Payload: payload,
	}
	return ch.conn.push(msg)
}

// GetRef linter
func (ch *Chan) GetRef() string {
	return ch.conn.ref.makeRef()
}

// Request send a msg to channel and return a MsgCh to receive reply
func (ch *Chan) Request(evt string, payload interface{}) (*Puller, error) {
	msg := &Message{
		Topic:   ch.topic,
		Event:   evt,
		Ref:     ch.conn.ref.makeRef(),
		Payload: payload,
	}
	// If we're receiving a reply, all we care about is that message references
	// Match. Connection.dispatch has a dispatcher for this key pattern ("","",msg.Ref)
	key := toKey("", "", msg.Ref)
	puller := ch.conn.center.register(key)
	if err := ch.conn.push(msg); err != nil {
		puller.Close()
		return nil, err
	}
	return puller, nil
}

// Join channel join, return a MsgCh to receive join result
func (ch *Chan) Join() (*Puller, error) {
	return ch.Request(ChanJoin, "")
}

// Leave channel leave, return a MsgCh to receive leave result
func (ch *Chan) Leave() (*Puller, error) {
	return ch.Request(ChanLeave, "")
}

// OnEvent return a MsgCh to receive all msg on some event on this channel
func (ch *Chan) OnEvent(evt string) *Puller {
	key := toKey(ch.topic, evt, "")
	return ch.conn.center.register(key)
}

// OnMessage register a MsgCh to recv all msg on channel
func (ch *Chan) OnMessage() *Puller {
	key := toKey(ch.topic, "", "")
	return ch.conn.center.register(key)
}
