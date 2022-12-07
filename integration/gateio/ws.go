package gateio

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

// https://www.gate.io/docs/developers/apiv4/en/#gate-api-v4-v4-22-2
var GateURL = "wss://api.gateio.ws/ws/v4/"

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
	Time    int64       `json:"time"`
	ID      int         `json:"id,omitempty"`
	Channel string      `json:"Channel"`
	Auth    WsAuth      `json:"auth,omitempty"`
	Event   string      `json:"event"`
	Payload interface{} `json:"payload,omitempty"`
}

type WsAuth struct {
	Method    string `json:"method"`
	ApiKey    string `json:"KEY"`
	Signature string `json:"SIGN"`
}

type WsResponse struct {
	Time    int64           `json:"time"`
	ID      int             `json:"id,omitempty"`
	Channel string          `json:"Channel"`
	Error   WsError         `json:"error,omitempty"`
	Event   string          `json:"event"`
	Result  json.RawMessage `json:"result"`
}

type WsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// https://www.gate.io/docs/developers/apiv4/ws/en/#changed-order-book-levels
type WsOrderBookUpdateResult struct {
	Pair string               `json:"s"`
	Bids []gateway.PriceArray `json:"b"`
	Asks []gateway.PriceArray `json:"a"`
}

// https://www.gate.io/docs/developers/apiv4/ws/en/#server-notification-7
type WsTradeResult struct {
	ID           int64   `json:"id"`
	CurrencyPair string  `json:"currency_pair"`
	Side         string  `json:"side"`
	Amount       float64 `json:"amount,string"`
	Price        float64 `json:"price,string"`
	CreatedTime  int64   `json:"created_time_ms,string"`
}

type WsOrderResult struct {
	ID               string  `json:"id"`
	CurrencyPair     string  `json:"currency_pair"`
	Status           string  `json:"status"`
	Price            float64 `json:"price,string"`
	Amount           float64 `json:"amount,string"`
	FilledMoneyValue float64 `json:"filled_total,string"`
	AmountLeftToFill float64 `json:"left,string"`
	Event            string  `json:event`
}

type WsOrderBookSnapshot struct {
	Pair         string               `json:"s"`
	LastUpdateID int64                `json:"lastUpdateId"`
	Time         int64                `json:"t"`
	Bids         []gateway.PriceArray `json:"bids"`
	Asks         []gateway.PriceArray `json:"asks"`
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

	ws, _, err := dialer.Dial(GateURL, nil)
	if err != nil {
		return err
	}

	session.conn = ws

	go session.websocketPinger()
	go session.messageHandler()

	return nil
}

// Note: According Gate.io docs, it's not required to send this request
// because Gate.io server already uses the protocol layer ping/pong.
// TODO: Check if we should keep it.
func (session *WsSession) websocketPinger() {
	sendPing := func() error {
		pingReq := WsRequest{
			Channel: "spot.ping",
			Time:    time.Now().UnixMilli(),
		}
		data, err := json.Marshal(pingReq)
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
				err = fmt.Errorf("Gate.io ws failed to send ping request. Err: %s", err)
				if session.options.Verbose {
					log.Printf("Gate.io ws failed to sendPing, err: %s", err)
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
				panic(fmt.Errorf("Gate.io failed to read from ws, err: %s", err))
			}()
			return
		}

		resp := WsResponse{}
		json.Unmarshal(data, &resp)
		if err != nil {
			log.Println("Gate.io websocket unmarhsal error", err)
			continue
		}

		if resp.Error.Code > 0 {
			log.Printf("Gate.io websocket error response, err: %v - %s", resp.Error.Code, resp.Error.Message)
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
