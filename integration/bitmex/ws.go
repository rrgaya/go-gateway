package bitmex

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

const (
	WsBase = "wss://www.bitmex.com/realtime"
	WsHost = "www.bitmex.com"
)

type WsSession struct {
	options          gateway.Options
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsResponse]bool
	subscribersMutex *sync.Mutex
	pingTimer        chan bool
}

type WsRequest struct {
	Op   string        `json:"op"`
	Args []interface{} `json:"args"`
}

type WsResponse struct {
	// Subscription response
	Subscribe string    `json:"subscribe"`
	Success   bool      `json:"success"`
	Request   WsRequest `json:"request"`

	// Errors response
	Error string `json:"error"`

	// Data response
	Table  string          `json:"table"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

type WsExecution struct {
	ExecID    string  `json:"execID"`
	OrderID   string  `json:"orderID"`
	ClOrdID   string  `json:"clOrdID"`
	OrdStatus string  `json:"ordStatus"`
	Symbol    string  `json:"symbol"`
	Side      string  `json:"side"`
	Price     float64 `json:"price"`
	AvgPx     float64 `json:"avgPx"`
	OrderQty  float64 `json:"orderQty"`
	CumQty    float64 `json:"cumQty"`
	LeavesQty float64 `json:"leavesQty"`
	ExecComm  float64 `json:"execComm"`
	LastQty   float64 `json:"lastQty"`
	LastPx    float64 `json:"lastPx"`
	ExecType  string  `json:"execType"`
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		options:          options,
		quit:             make(chan bool),
		subscribers:      make(map[chan WsResponse]bool),
		subscribersMutex: &sync.Mutex{},
		pingTimer:        make(chan bool),
	}
}

func (w *WsSession) addSubscriber(ch chan WsResponse) {
	w.subscribersMutex.Lock()
	defer w.subscribersMutex.Unlock()

	w.subscribers[ch] = true
}

func (w *WsSession) removeSubscriber(ch chan WsResponse) {
	w.subscribersMutex.Lock()
	defer w.subscribersMutex.Unlock()

	delete(w.subscribers, ch)
}

func (w *WsSession) Connect() error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	var addr string
	var host string

	if w.options.WsAddr != "" {
		addr = w.options.WsAddr
	} else {
		addr = WsBase
	}
	if w.options.WsHost != "" {
		host = w.options.WsHost
	} else {
		host = WsHost
	}

	headers := http.Header{
		"Host": []string{host},
	}

	ws, res, err := dialer.Dial(addr, headers)
	if err != nil {
		log.Printf("Dial to %s %s failed, http res %s, err: %s", addr, host, res.Status, err)
		return err
	}

	w.conn = ws
	w.connWriterMutex = &sync.Mutex{}

	go w.websocketPinger()
	go w.messageHandler()

	return nil
}

func (w *WsSession) Close() {
	close(w.quit)
	w.conn.Close()
}

func (w *WsSession) resetPingTimer() {
	select {
	case w.pingTimer <- true:
	default:
	}
}

func (w *WsSession) messageHandler() {
	for {
		_, data, err := w.conn.ReadMessage()
		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				panic(fmt.Errorf("BitMEX failed to read from ws, err: %s", err))
			}()
			return
		}

		if string(data) == "pong" {
			//log.Printf("received pong")
		}

		resp := WsResponse{}
		json.Unmarshal(data, &resp)
		if err != nil {
			log.Println("BitMEX websocket unmarhsal error", err)
			continue
		}

		if resp.Error != "" {
			log.Printf("Bitmex ws err message: %s", resp.Error)
		}

		// Check for subscribers
		w.subscribersMutex.Lock()
		for ch := range w.subscribers {
			ch <- resp
		}
		w.subscribersMutex.Unlock()

		// Reset pinger, connection not dead
		w.resetPingTimer()
	}
}

func (w *WsSession) websocketPinger() {
	for {
		select {
		case <-time.After(5 * time.Second):
			err := w.WriteMessage([]byte("ping"))
			if err != nil {
				log.Printf("BitMEX ws failed to send ping, err: %s", err)
				w.Close()
				return
			}
		case <-w.pingTimer:
			continue
		case <-w.quit:
			return
		}
	}
}

func (w *WsSession) Authenticate(apiKey, apiSecret string) error {
	// Calc signature
	expires := time.Now().Unix() + 3 // Request expires in 3 seconds
	data := "GET/realtime" + strconv.FormatInt(expires, 10)
	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))

	req := WsRequest{
		Op:   "authKeyExpires",
		Args: []interface{}{apiKey, expires, signature},
	}

	authErr := make(chan error)
	authSuccess := make(chan bool)
	res := make(chan WsResponse)

	go func() {
		for res := range res {
			// Check for auth response
			if res.Request.Op == "authKeyExpires" {
				if res.Success == true {
					authSuccess <- true
				} else {
					authErr <- fmt.Errorf("%s", res.Error)
				}
			}
		}
	}()

	// Register subscriber
	w.addSubscriber(res)
	defer w.removeSubscriber(res)

	// Send request, and wait for response
	err := w.SendRequest(req)
	if err != nil {
		return err
	}

	// Wait for auth response
	select {
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for 5 seconds for auth response")
	case err := <-authErr:
		return fmt.Errorf("auth returned err: %s", err)
	case <-authSuccess:
		return nil
	}
}

func (w *WsSession) SubscribeExecutions(ch chan []WsExecution, quit chan bool) error {
	req := WsRequest{
		Op:   "subscribe",
		Args: []interface{}{"execution"},
	}

	subErr := make(chan error)
	res := make(chan WsResponse)

	go func() {
		for res := range res {
			// Check for auth response
			if res.Subscribe == "execution" {
				if res.Success == true {
					subErr <- nil
				} else {
					subErr <- fmt.Errorf("failed to sub to orders, err: %s", res.Error)
				}
			}

			if res.Table == "execution" && res.Action != "partial" {
				var execs []WsExecution
				err := json.Unmarshal(res.Data, &execs)
				if err != nil {
					log.Printf("Bitmex failed to unmarshal executions update data, err: %s, data: %s", err, string(res.Data))
					continue
				}

				// Dispatch order update
				ch <- execs
			}
		}
	}()

	// Register subscriber
	w.addSubscriber(res)

	// Quit, cleanup
	if quit != nil {
		go func() {
			<-quit
			w.removeSubscriber(res)
			close(res)
		}()
	}

	// Send request, and wait for response
	err := w.SendRequest(req)
	if err != nil {
		return err
	}

	// Wait for auth response
	select {
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for 5 seconds for executions subscribe response")
	case err := <-subErr:
		return err
	}
}

func (w *WsSession) SubscribeMessages(ch chan WsResponse, quit chan bool) {
	// Register subscriber
	w.addSubscriber(ch)

	// Quit, cleanup
	if quit != nil {
		go func() {
			<-quit
			w.removeSubscriber(ch)
		}()
	}
}

func (w *WsSession) RequestSubscriptions(topics []string) error {
	args := make([]interface{}, len(topics))
	for i, v := range topics {
		args[i] = v
	}

	req := WsRequest{
		Op:   "subscribe",
		Args: args,
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

	// Reset pinger, connection not dead
	w.resetPingTimer()

	return nil
}
