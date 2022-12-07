package bitglobal

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

var WsRequestPath = "/message/realtime"
var WsUrl = "wss://global-api.bithumb.pro" + WsRequestPath

type WsSession struct {
	options          gateway.Options
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quitPinger       chan bool
	subscribers      map[chan WsGenericMessage]bool
	subscribersMutex *sync.Mutex
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		options:          options,
		quitPinger:       make(chan bool),
		subscribers:      make(map[chan WsGenericMessage]bool),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
	}
}

func (ws *WsSession) Connect() error {
	dialer := websocket.Dialer{}

	conn, _, err := dialer.Dial(WsUrl, nil)
	if err != nil {
		return err
	}

	ws.conn = conn

	go ws.messageHandler()
	go ws.websocketPinger()

	return nil
}

func (ws *WsSession) websocketPinger() {
	for {
		select {
		case <-time.After(5 * time.Second):
			err := ws.sendPing()
			if err != nil {
				log.Printf("Bitglobal ws failed to send ping request, err: %s", err)
			}
		case <-ws.quitPinger:
			return
		}
	}
}

func (ws *WsSession) sendPing() error {
	return ws.WriteMessage([]byte("{\"cmd\":\"ping\"}"))
}

type WsRequest struct {
	Cmd  string      `json:"cmd"`
	Args interface{} `json:"args"`
}

type WsGenericMessage struct {
	Code  string `json:"code"`
	Topic string `json:"topic"`
	Data  json.RawMessage
}

type WsOrder struct {
	DealPrice    float64 `json:"dealPrice,string"`
	DealQuantity float64 `json:"dealQuantity,string"`
	DealVolume   float64 `json:"dealVolume,string"`
	Fee          float64 `json:"fee,string"`
	FeeType      string  `json:"feeType"`
	OID          string  `json:"oId"`
	Price        float64 `json:"price,string"`
	Quantity     float64 `json:"quantity,string"`
	Side         string  `json:"side"` // buy or sell
	Status       string  `json:"status"`
	Symbol       string  `json:"symbol"`
	Type         string  `json:"type"` // limit or market

}

type WsDepth struct {
	Bids   []gateway.PriceArray `json:"b"`
	Asks   []gateway.PriceArray `json:"s"`
	Symbol string               `json:"symbol"`
}

type WsTrade struct {
	Price    float64 `json:"p,string"`
	Quantity float64 `json:"v,string"`
	Side     string  `json:"s"`
	Symbol   string  `json:"symbol"`
	Ver      string  `json:"ver"`
}

func (ws *WsSession) messageHandler() {
	for {
		_, data, err := ws.conn.ReadMessage()
		if err != nil {
			close(ws.quitPinger)
			panic(fmt.Errorf("XT failed to read from ws, err: %s", err))
		}

		var msg WsGenericMessage
		err = json.Unmarshal(data, &msg)
		if err != nil {
			log.Printf("Failed to unmarhsal wsGenericMessage [%s] err [%s]", string(data), err)
		}

		// Check for errs
		errCodeInt, _ := strconv.ParseInt(msg.Code, 10, 64)
		if errCodeInt >= 10000 {
			log.Printf("BitGlobal ws msg w/ error code [%d] msg: %s", errCodeInt, string(data))
		}

		ws.subscribersMutex.Lock()
		for ch := range ws.subscribers {
			ch <- msg
		}
		ws.subscribersMutex.Unlock()
	}
}

func (ws *WsSession) WriteMessage(data []byte) error {
	ws.connWriterMutex.Lock()
	err := ws.conn.WriteMessage(websocket.TextMessage, data)
	ws.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func (ws *WsSession) SubscribeMessages(ch chan WsGenericMessage, quit chan bool) {
	ws.addSubscriber(ch)

	if quit != nil {
		go func() {
			<-quit
			ws.removeSubscriber(ch)
		}()
	}
}

func (ws *WsSession) addSubscriber(ch chan WsGenericMessage) {
	ws.subscribersMutex.Lock()
	defer ws.subscribersMutex.Unlock()

	ws.subscribers[ch] = true
}

func (ws *WsSession) removeSubscriber(ch chan WsGenericMessage) {
	ws.subscribersMutex.Lock()
	defer ws.subscribersMutex.Unlock()

	delete(ws.subscribers, ch)
}

func (s *WsSession) SendRequest(request WsRequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}

	return s.WriteMessage(data)
}

func (ws *WsSession) Authenticate(apiKey, apiSecret string) error {
	timestampMilli := time.Now().UnixMilli()
	timestampStr := strconv.FormatInt(timestampMilli, 10)

	signatureStr := WsRequestPath + timestampStr + ws.options.ApiKey

	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(signatureStr))
	signature := hex.EncodeToString(h.Sum(nil))

	authReq := WsRequest{
		Cmd: "authKey",
		Args: []interface{}{
			apiKey,
			timestampStr,
			signature,
		},
	}

	// Check for response
	ch := make(chan WsGenericMessage, 10)
	unsub := make(chan bool, 1)
	ws.SubscribeMessages(ch, unsub)
	authSuccess := make(chan bool)
	go func() {
		for msg := range ch {
			if msg.Code == "00000" {
				authSuccess <- true
			}
		}
	}()

	err := ws.SendRequest(authReq)
	if err != nil {
		unsub <- true
		return fmt.Errorf("send request err: %s", err)
	}

	// Wait for auth
	select {
	case <-authSuccess:
		log.Printf("Authenticated with ws account...")
	case <-time.After(5 * time.Second):
		unsub <- true
		return fmt.Errorf("auth timeout after 5 seconds...")
	}

	unsub <- true

	return nil
}
