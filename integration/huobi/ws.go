package huobi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

var HuobiWsUrl = "wss://api.huobi.pro/ws/v2"

type WsSession struct {
	id                int
	options           gateway.Options
	conn              *websocket.Conn
	subscriptions     map[string]map[chan WsMessage]struct{}
	subscriptionsLock *sync.Mutex
	lastPong          time.Time
	quit              chan bool
}

func NewWsSession(id int, options gateway.Options) *WsSession {
	return &WsSession{
		id:                id,
		options:           options,
		subscriptions:     make(map[string]map[chan WsMessage]struct{}),
		subscriptionsLock: &sync.Mutex{},
		quit:              make(chan bool),
	}
}

type WsMessage struct {
	Action string `json:"action"`
	Code   int64  `json:"code"`
	Ch     string `json:"ch"`
	Data   json.RawMessage
	Params interface{} `json:"params,omitempty"`
}

type WsAuth struct {
	AuthType         string `json:"authType"`
	AccessKey        string `json:"accessKey"`
	SignatureMethod  string `json:"signatureMethod"`
	SignatureVersion string `json:"signatureVersion"`
	Timestamp        string `json:"timestamp"`
	Signature        string `json:"signature"`
}

func (w *WsSession) Connect() error {
	dialer := websocket.Dialer{}

	ws, _, err := dialer.Dial(HuobiWsUrl, nil)
	if err != nil {
		return err
	}

	w.conn = ws

	go w.messageHandler()

	return nil
}

func (w *WsSession) Close() {
	log.Printf("Huobi closing ws (%d)...", w.id)

	close(w.quit)

	w.conn.Close()
}

type WsPing struct {
	Timestamp int64 "json:`ts`"
}

func (w *WsSession) messageHandler() {
	ch := make(chan WsMessage)
	w.SubscribeChannel("ping:", ch)
	go func() {
		for msg := range ch {
			ping := WsPing{}
			err := json.Unmarshal(msg.Data, &ping)
			if err != nil {
				log.Printf("Failed to unmarshal ping msg: %s", err)
				continue
			}

			err = w.sendPong(ping)
			if err != nil {
				log.Printf("Huobi failed to send pong, err: %s", err)
				continue
			}
		}
	}()

	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			panic(fmt.Errorf("Huobi failed to read from websocket, err: %s", err))
		}

		w.processWebsocketMessage(message)
	}
}

func (w *WsSession) processWebsocketMessage(data []byte) {
	var message WsMessage
	err := json.Unmarshal(data, &message)
	if err != nil {
		log.Println("Huobi websocket unmarhsal error", err)
		return
	}

	subscribers, ok := w.subscriptions[message.Action+":"+message.Ch]
	if ok {
		for ch, _ := range subscribers {
			go func() { ch <- message }()
		}
	} else {
		log.Printf("Huobi recevied [%s] [%s] message, but had no subscribers listening on topic.", message.Action, message.Ch)
	}
}

func (w *WsSession) SendMessage(message WsMessage) error {
	// Append the request cid to the message (we will need to unmarshal/marshal twice)
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return w.conn.WriteMessage(websocket.TextMessage, data)
}

func (w *WsSession) SubscribeChannel(ch string, sub chan WsMessage) {
	w.subscriptionsLock.Lock()
	defer w.subscriptionsLock.Unlock()

	_, ok := w.subscriptions[ch]
	if !ok {
		w.subscriptions[ch] = make(map[chan WsMessage]struct{})
	}

	w.subscriptions[ch][sub] = struct{}{}
}

func (w *WsSession) UnsubscribeChannel(ch string, sub chan WsMessage) {
	w.subscriptionsLock.Lock()
	defer w.subscriptionsLock.Unlock()

	_, ok := w.subscriptions[ch]
	if ok {
		delete(w.subscriptions[ch], sub)
	}
}

func (w *WsSession) sendPong(ping WsPing) error {
	data, err := json.Marshal(ping)
	if err != nil {
		return err
	}

	msg := WsMessage{
		Action: "pong",
		Data:   data,
	}

	return w.SendMessage(msg)
}

func (w *WsSession) Authenticate() error {
	apiKey := w.options.ApiKey
	apiSecret := w.options.ApiSecret
	signatureMethod := "HmacSHA256"
	signatureVersion := "2.1"
	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05")

	values := url.Values{}
	values.Set("accessKey", apiKey)
	values.Set("signatureMethod", signatureMethod)
	values.Set("signatureVersion", signatureVersion)
	values.Set("timestamp", timestamp)

	method := "GET"
	host := "api.huobi.pro"
	endpoint := "/ws/v2"

	payload := fmt.Sprintf(
		"%s\n%s\n%s\n%s",
		method,
		host,
		endpoint,
		values.Encode(),
	)

	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(payload))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	auth := WsMessage{
		Action: "req",
		Ch:     "auth",
		Params: WsAuth{
			AuthType:         "api",
			AccessKey:        apiKey,
			SignatureMethod:  signatureMethod,
			SignatureVersion: signatureVersion,
			Timestamp:        timestamp,
			Signature:        signature,
		},
	}

	ch := make(chan WsMessage)
	w.SubscribeChannel("req:auth", ch)
	defer w.UnsubscribeChannel("req:auth", ch)

	err := w.SendMessage(auth)
	if err != nil {
		return err
	}

	select {
	case res := <-ch:
		if res.Code != 200 {
			return fmt.Errorf("auth failed, not 200 response, res: %+v", res)
		}
		return nil
	case <-time.After(15 * time.Second):
		return fmt.Errorf("timed out waiting for auth ressponse after 15 seconds...")
	}
}

func (w *WsSession) SubscribeSub(topic string, ch chan WsMessage, quit chan bool) error {
	msg := WsMessage{
		Action: "sub",
		Ch:     topic,
	}

	key := fmt.Sprintf("push:%s", topic)
	w.SubscribeChannel(key, ch)

	if quit != nil {
		go func() {
			<-quit
			w.UnsubscribeChannel(key, ch)
		}()
	}

	return w.SendMessage(msg)
}
