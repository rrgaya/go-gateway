package bitso

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

var bitsoURL = "wss://ws.bitso.com"

type WsSession struct {
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsResponse]bool
	subscribersMutex *sync.Mutex
}

type WsRequest struct {
	Action string `json:"action"`
	Symbol string `json:"book"`
	Type   string `json:"type"`
}

type WsResponse struct {
	Type     string          `json:"type"`
	Symbol   string          `json:"book"`
	Sequence int             `json:"sequence,omitempty"`
	Payload  json.RawMessage `json:"payload"`
}

type WsTrade struct {
	Transaction   int64   `json:"i"`
	Amount        float64 `json:"a,string"`
	Rate          float64 `json:"r,string"`
	Value         float64 `json:"v,string"`
	MakerSide     int8    `json:"t"`
	MarketOrderID string  `json:"mo"`
	TackerOrderID string  `json:"to"`
}

type WsOrder struct {
	Timestamp int64       `json:"d"`
	Rate      json.Number `json:"r"`
	Side      int8        `json:"t"`
	Amount    json.Number `json:"a"`
	Value     json.Number `json:"v"`
	OrderID   string      `json:"o,omitempty"`
	Status    string      `json:"s,omitempty"`
}

type WsOrders struct {
	Bids []WsOrder `json:"bids"`
	Asks []WsOrder `json:"asks"`
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		quit:             make(chan bool),
		subscribers:      make(map[chan WsResponse]bool),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
	}
}

func (session *WsSession) Connect() error {
	dialer := websocket.Dialer{}

	ws, _, err := dialer.Dial(bitsoURL, nil)
	if err != nil {
		return err
	}

	session.conn = ws

	go session.messageHandler()

	return nil
}

func (session *WsSession) messageHandler() {
	for {
		_, data, err := session.conn.ReadMessage()
		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				panic(fmt.Errorf("Bitso failed to read from ws, err: %s", err))
			}()
			return
		}

		resp := WsResponse{}
		json.Unmarshal(data, &resp)
		if err != nil {
			log.Println("Bitso websocket unmarhsal error", err)
			continue
		}

		session.subscribersMutex.Lock()
		for ch := range session.subscribers {
			ch <- resp
		}
		session.subscribersMutex.Unlock()
	}
}

func (session *WsSession) SendRequest(request WsRequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}

	return session.WriteMessage(data)
}

func (session *WsSession) WriteMessage(data []byte) error {
	session.connWriterMutex.Lock()
	err := session.conn.WriteMessage(websocket.TextMessage, data)
	session.connWriterMutex.Unlock()

	if err != nil {
		return err
	}

	return nil
}

func (session *WsSession) RequestSubscriptions(requests []WsRequest) error {
	for _, req := range requests {
		err := session.SendRequest(req)
		if err != nil {
			return err
		}
	}

	return nil
}

func (session *WsSession) SubscribeMessages(ch chan WsResponse, quit chan bool) {
	session.addSubscriber(ch)

	if quit != nil {
		go func() {
			<-quit
			session.removeSubscriber(ch)
		}()
	}
}

func (session *WsSession) addSubscriber(ch chan WsResponse) {
	session.subscribersMutex.Lock()
	defer session.subscribersMutex.Unlock()

	session.subscribers[ch] = true
}

func (session *WsSession) removeSubscriber(ch chan WsResponse) {
	session.subscribersMutex.Lock()
	defer session.subscribersMutex.Unlock()

	delete(session.subscribers, ch)
}
