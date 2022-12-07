package bitz

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

const (
	wsBase = "wss://ws.ahighapi.com/wss"
)

type WsMessage struct {
	MsgId  int64           `json:"msgId"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
	Params json.RawMessage `json:"params"`
}

type DepthUpdate struct {
	Symbol string
	Asks   []gateway.PriceArray
	Bids   []gateway.PriceArray
}

type WS struct {
	conn                    *websocket.Conn
	connWriterMutex         *sync.Mutex
	messageSubscribers      map[int64]chan WsMessage
	messageSubscribersMutex *sync.RWMutex
	depthUpdate             chan DepthUpdate // Subscribe to market depth updates
	quit                    chan bool
}

func NewWS() *WS {
	return &WS{
		messageSubscribers:      make(map[int64]chan WsMessage),
		messageSubscribersMutex: &sync.RWMutex{},
		quit:                    make(chan bool),
	}
}

func (w *WS) Connect() error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	ws, _, err := dialer.Dial(wsBase, nil)
	if err != nil {
		return err
	}

	w.conn = ws
	w.connWriterMutex = &sync.Mutex{}

	go w.websocketPinger()
	go w.messageHandler()

	return nil
}

func (w *WS) Close() {
	log.Printf("closing ws...")
	close(w.quit)
	w.conn.Close()
}

func (w *WS) messageHandler() {
	for {
		_, data, err := w.conn.ReadMessage()
		if err != nil {
			panic(fmt.Errorf("failed to read from ws, err: %s", err))
		}

		if string(data) == "pong" {
			continue
		}

		msg := WsMessage{}
		json.Unmarshal(data, &msg)
		if err != nil {
			log.Println("websocket unmarhsal error", err)
			continue
		}

		if msg.MsgId != 0 {
			w.dispatchMsgToSubscriber(msg.MsgId, msg)
			continue
		}

		switch msg.Action {
		case "Pushdata.depth":
			w.dispatchDepthUpdate(msg)
		}
	}
}

func (w *WS) dispatchMsgToSubscriber(msgId int64, msg WsMessage) {
	w.messageSubscribersMutex.RLock()
	subscriber, ok := w.messageSubscribers[msgId]
	w.messageSubscribersMutex.RUnlock()
	if ok {
		go func() {
			subscriber <- msg
		}()

		// Unsubscribe, we won't receive messages with this id anymore
		w.unsubscribeMessageId(msgId)
	}
}

func (w *WS) dispatchDepthUpdate(msg WsMessage) {
	if w.depthUpdate == nil {
		return
	}

	var depthUpdate DepthUpdate
	err := json.Unmarshal(msg.Data, &depthUpdate)
	if err != nil {
		log.Printf("bitz ws failed to unmarshal depth update data: %s, unmarshal error: %s", msg.Data, err)
		return
	}

	symbol, err := jsonparser.GetString(msg.Params, "symbol")
	if err != nil {
		log.Printf("bitz ws failed to parse symbol from msg params: %s, error: %s", msg.Params, err)
		return
	}

	depthUpdate.Symbol = symbol

	w.depthUpdate <- depthUpdate
}

func (w *WS) websocketPinger() {
	for {
		select {
		case <-time.After(30 * time.Second):
			err := w.WriteMessage([]byte("ping"))
			if err != nil {
				log.Printf("ws failed to send ping, err: %s", err)
				w.Close()
				return
			}
		case <-w.quit:
			return
		}
	}
}

func (w *WS) SubscribeMarketDepth(symbol string) (DepthUpdate, error) {
	msgId := genMsgId()
	msg := fmt.Sprintf(
		"{\"data\":{\"symbol\":\"%s\",\"type\":\"%s\",\"dataType\":\"blob\"},\"msgId\":%d,\"action\":\"Topic.sub\"}",
		symbol,
		"depth",
		msgId,
	)

	ch := w.subscribeMessageId(msgId)
	err := w.WriteMessage([]byte(msg))
	if err != nil {
		w.unsubscribeMessageId(msgId)
		return DepthUpdate{}, err
	}

	select {
	case <-time.After(15 * time.Second):
		return DepthUpdate{}, fmt.Errorf("timed out after 15 seconds waiting for msg res")
	case msg := <-ch:
		var depthUpdate DepthUpdate
		err := json.Unmarshal(msg.Data, &depthUpdate)
		if err != nil {
			return DepthUpdate{}, fmt.Errorf("failed to unmarshal depth update data: %s, unmarshal error: %s", msg.Data, err)
		}

		symbol, err := jsonparser.GetString(msg.Params, "symbol")
		if err != nil {
			return DepthUpdate{}, fmt.Errorf("failed to parse symbol from msg params: %s, error: %s", msg.Params, err)
		}

		depthUpdate.Symbol = symbol

		return depthUpdate, nil
	}
}

func (w *WS) subscribeMessageId(msgId int64) chan WsMessage {
	ch := make(chan WsMessage)

	w.messageSubscribersMutex.Lock()
	w.messageSubscribers[msgId] = ch
	w.messageSubscribersMutex.Unlock()

	return ch
}

func (w *WS) unsubscribeMessageId(msgId int64) {
	w.messageSubscribersMutex.Lock()
	delete(w.messageSubscribers, msgId)
	w.messageSubscribersMutex.Unlock()
}

func (w *WS) WriteMessage(data []byte) error {
	w.connWriterMutex.Lock()
	err := w.conn.WriteMessage(websocket.TextMessage, data)
	w.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func genMsgId() int64 {
	return rand.Int63()
}
