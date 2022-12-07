package lbank

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

var WsUrl = "wss://www.lbkex.net/ws/V2/"

type WsSession struct {
	options          gateway.Options
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsGenericMessage]bool
	subscribersMutex *sync.Mutex
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		options:          options,
		quit:             make(chan bool),
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

	return nil
}

func (ws *WsSession) sendPing(pong string) error {
	return ws.WriteMessage([]byte("{\"action\":\"pong\", \"pong\":\"" + pong + "\"}"))
}

type WsGenericMessage struct {
	Type   string `json:"type"`
	Action string `json:"action"`
	Ping   string `json:"ping"`
	Status string `json:"status"`
	Data   []byte
}

func (ws *WsSession) messageHandler() {
	for {
		_, data, err := ws.conn.ReadMessage()
		if err != nil {
			panic(fmt.Errorf("LBank failed to read from ws, err: %s", err))
		}

		var msg WsGenericMessage
		err = json.Unmarshal(data, &msg)
		if err != nil {
			log.Printf("Failed to unmarhsal wsActionMsg [%s] err [%s]", string(data), err)
		}

		// Check for errs
		if msg.Status == "error" {
			log.Printf("LBank ws rcvd error msg: %s", string(data))
		}

		// Check if ping request
		if msg.Action == "ping" {
			err := ws.sendPing(msg.Ping)
			if err != nil {
				log.Printf("Failed to send ping [%s] res, err: %s", msg.Ping, err)
			}
		}

		// Append data
		msg.Data = data

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
