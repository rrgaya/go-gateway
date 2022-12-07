package chiliz

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

// https://chiliz.zendesk.com/hc/en-us/articles/360011405440-Chiliz-net-Websocket-Stream
var chilizURL = "wss://wsapi.chiliz.net/openapi/quote/ws/v1"

type WsSession struct {
	options          gateway.Options
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsResponse]bool
	subscribersMutex *sync.Mutex
}

type WsHeartbeatReq struct {
	Ping int64 `json:"ping"`
}

type WsRequest struct {
	Symbol string      `json:"symbol"`
	Topic  string      `json:"topic"`
	Event  string      `json:"event"`
	Params interface{} `json:"params,omitempty"`
}

type WsResponse struct {
	Symbol     string          `json:"symbol"`
	Topic      string          `json:"topic"`
	Data       json.RawMessage `json:"data"`
	FirstEntry bool            `json:"f"`
}

type WsDiffDepthDataResponse struct {
	Timestamp int64                `json:"t"`
	ID        string               `json:"v"`
	Bids      []gateway.PriceArray `json:"b"`
	Asks      []gateway.PriceArray `json:"a"`
}

type WsTradeDataResponse struct {
	ID        string  `json:"v"`
	Timestamp int64   `json:"t"`
	Price     float64 `json:"price"`
	Amount    float64 `json:"q"`
	Buying    bool    `json:"m"`
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		options:          options,
		quit:             make(chan bool),
		subscribers:      make(map[chan WsResponse]bool),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
	}
}

func (session *WsSession) Connect() error {
	dialer := websocket.Dialer{}

	ws, _, err := dialer.Dial(chilizURL, nil)
	if err != nil {
		return err
	}

	session.conn = ws

	go session.WebsocketPinger()
	go session.messageHandler()

	return nil
}

func (session *WsSession) WebsocketPinger() {
	sendPing := func() error {
		ping := WsHeartbeatReq{}
		data, err := json.Marshal(ping)
		if err != nil {
			log.Println(err)
		}

		err = session.conn.WriteMessage(websocket.TextMessage, data)

		return err
	}

	for {
		select {
		case <-time.After(30 * time.Second):
			err := sendPing()
			if err != nil {
				err = fmt.Errorf("Chiliz ws failed to send ping request. Err: %s", err)
				if session.options.Verbose {
					log.Printf("Chiliz ws failed to sendPing, err: %s", err)
				}
			}
		case <-session.quit:
			return
		}
	}
}

func (session *WsSession) messageHandler() {
	for {
		_, data, err := session.conn.ReadMessage()
		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				panic(fmt.Errorf("Chiliz failed to read from ws, err: %s", err))
			}()
			return
		}

		resp := WsResponse{}
		json.Unmarshal(data, &resp)
		if err != nil {
			log.Println("Chiliz websocket unmarhsal error", err)
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
