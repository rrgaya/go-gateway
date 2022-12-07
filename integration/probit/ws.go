package probit

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	WsURL = "wss://api.probit.com/api/exchange/v1/ws"
)

type WsSession struct {
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsMessage]string
	subscribersMutex *sync.Mutex
	waitingPong      bool
}

type WsRequest struct {
	Type     string   `json:"type"`
	Channel  string   `json:"channel"`
	Interval int64    `json:"interval"`
	MarketID string   `json:"market_id"`
	Filter   []string `json:"filter"`
	Token    string   `json:"token"`
}

type WsMessage struct {
	Type       string          `json:"type"`
	Channel    string          `json:"channel"`
	MarketID   string          `json:"market_id"`
	Status     string          `json:"status"`
	Lag        int64           `json:"lag"`
	ErrorCode  string          `json:"errorCode"`
	OrderBooks json.RawMessage `json:"order_books_l0"`
	Result     string          `json:"result"`
	Data       json.RawMessage `json:"data"`
	Reset      bool            `json:"reset"`
}

type WsBookRow struct {
	Side     string  `json:"side"`
	Price    float64 `json:"price,string"`
	Quantity float64 `json:"quantity,string"`
}

func NewWsSession() *WsSession {
	return &WsSession{
		quit:             make(chan bool),
		subscribers:      make(map[chan WsMessage]string),
		subscribersMutex: &sync.Mutex{},
	}
}

func (w *WsSession) addSubscriber(ch chan WsMessage, channel string) {
	w.subscribersMutex.Lock()
	defer w.subscribersMutex.Unlock()

	w.subscribers[ch] = channel
}

func (w *WsSession) removeSubscriber(ch chan WsMessage) {
	w.subscribersMutex.Lock()
	defer w.subscribersMutex.Unlock()

	delete(w.subscribers, ch)
}

func (w *WsSession) Connect() error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	ws, _, err := dialer.Dial(WsURL, nil)
	if err != nil {
		return err
	}

	w.conn = ws
	w.connWriterMutex = &sync.Mutex{}

	go w.messageHandler()

	return nil
}

func (w *WsSession) Close() {
	close(w.quit)
	w.conn.Close()
}

func (w *WsSession) messageHandler() {
	for {
		_, data, err := w.conn.ReadMessage()
		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				err = fmt.Errorf("Probit failed to read from ws, err: %s", err)
				log.Println(err)
				time.Sleep(1000 * time.Millisecond)
				panic(err)
			}()
			return
		}

		msg := WsMessage{}
		json.Unmarshal(data, &msg)
		if err != nil {
			log.Println("ProBit websocket unmarhsal error", err)
			continue
		}

		if msg.ErrorCode != "" {
			if w.waitingPong {
				w.waitingPong = false
			} else {
				log.Printf("ProBit ws error message: %s", msg.ErrorCode)
			}
		}

		// Check for subscribers
		w.subscribersMutex.Lock()
		for ch, key := range w.subscribers {
			if key == msg.Channel || msg.Type == key {
				ch <- msg
			}
		}
		w.subscribersMutex.Unlock()
	}
}

func (w *WsSession) Authenticate(token string) error {
	req := WsRequest{
		Type:  "authorization",
		Token: token,
	}

	// Register subscriber
	ch := make(chan WsMessage)
	w.addSubscriber(ch, "authorization")

	errCh := make(chan error)
	go func() {
		msg := <-ch
		if msg.Result == "ok" {
			errCh <- nil
		} else {
			errCh <- fmt.Errorf("received unsuccessful auth response, msg: %+v", msg)
		}
		w.removeSubscriber(ch)
	}()

	// Send request, and wait for response
	err := w.SendRequest(req)
	if err != nil {
		return err
	}

	select {
	case err := <-errCh:
		return err
	case <-time.After(15 * time.Second):
		return fmt.Errorf("timedout while waiting for ws auth response")
	}
}

func (w *WsSession) SubscribeOrderBook(symbol string, ch chan WsMessage, quit chan bool) error {
	req := WsRequest{
		Type:     "subscribe",
		MarketID: symbol,
		Channel:  "marketdata",
		Filter:   []string{"order_books_l0"},
	}

	// Register subscriber
	w.addSubscriber(ch, "marketdata")

	// Quit, cleanup
	if quit != nil {
		go func() {
			<-quit

			req := WsRequest{
				Type:     "unsubscribe",
				Channel:  "marketdata",
				MarketID: symbol,
			}
			err := w.SendRequest(req)
			if err != nil {
				log.Printf("Failed to unsubscribe from market %s, err: %s", symbol, err)
			}

			w.removeSubscriber(ch)
		}()
	}

	// Send request, and wait for response
	err := w.SendRequest(req)
	if err != nil {
		return err
	}

	return nil
}

func (w *WsSession) SubscribeOpenOrder(ch chan WsMessage, quit chan bool) error {
	req := WsRequest{
		Type:    "subscribe",
		Channel: "open_order",
	}

	// Register subscriber
	w.addSubscriber(ch, "open_order")

	// Quit, cleanup
	if quit != nil {
		go func() {
			<-quit

			req := WsRequest{
				Type:    "unsubscribe",
				Channel: "open_order",
			}
			err := w.SendRequest(req)
			if err != nil {
				log.Printf("Failed to unsubscribe from user open_order, err: %s", err)
			}

			w.removeSubscriber(ch)
		}()
	}

	// Send request, and wait for response
	err := w.SendRequest(req)
	if err != nil {
		return err
	}

	return nil
}

func (w *WsSession) SendRequest(request WsRequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}

	return w.WriteMessage(data)
}

func (w *WsSession) WriteMessage(data []byte) error {
	w.connWriterMutex.Lock()
	err := w.conn.WriteMessage(websocket.TextMessage, data)
	w.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}
