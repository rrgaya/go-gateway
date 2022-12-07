package mercadobitcoin

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

var WsUrl = "wss://ws.mercadobitcoin.net/ws"

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
				log.Printf("MercadoBitcoin ws failed to send ping request, err: %s", err)
			}
		case <-ws.quitPinger:
			return
		}
	}
}

func (ws *WsSession) sendPing() error {
	return ws.WriteMessage([]byte("{\"type\":\"ping\"}"))
}

type WsRequest struct {
	Type string `json:"type"`
}

type WsGenericMessage struct {
	Type    string          `json:"type"`
	Message string          `json:"message"`
	ID      string          `json:"id"`
	TS      int64           `json:"ts"`
	Data    json.RawMessage `json:"data"`
}

type WsDepth struct {
	Asks []gateway.PriceArray `json:"asks"`
	Bids []gateway.PriceArray `json:"bids"`
}

type WsTrade struct {
	Date   int64   `json:"date"`
	TID    int64   `json:"tid"`
	Type   string  `json:"type"`
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

func (ws *WsSession) messageHandler() {
	for {
		_, data, err := ws.conn.ReadMessage()
		if err != nil {
			close(ws.quitPinger)
			panic(fmt.Errorf("MercadoBitcoin failed to read from ws, err: %s", err))
		}

		var msg WsGenericMessage
		err = json.Unmarshal(data, &msg)
		if err != nil {
			log.Printf("Failed to unmarhsal wsGenericMessage [%s] err [%s]", string(data), err)
		}

		// Check for errs
		if msg.Type == "error" {
			log.Printf("MercadoBitcoin ws msg w/ error msg [%s] msg: %s", msg.Message, string(data))
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
