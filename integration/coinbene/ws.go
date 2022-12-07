package coinbene

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
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

const (
	WsURL = "wss://ws.coinbene.com/stream/ws"
)

type WsSession struct {
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsMessage]struct{}
	subscribersMutex *sync.Mutex
	pingTimer        chan bool
}

type WsRequest struct {
	Op   string        `json:"op"`
	Args []interface{} `json:"args"`
}

type WsMessage struct {
	// Subscription response
	Event   string `json:"event"`
	Topic   string `json:"topic"`
	Success bool   `json:"success"`

	// Errors response
	Message string `json:"message"`
	Code    int64  `json:"code"`

	// Data response
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

type WsBook struct {
	Asks      [][2]string
	Bids      [][2]string
	Version   int64 `json:"version"`
	Timestamp int64 `json:"timestamp"`
}

func NewWsSession() *WsSession {
	return &WsSession{
		quit:             make(chan bool),
		subscribers:      make(map[chan WsMessage]struct{}),
		subscribersMutex: &sync.Mutex{},
		pingTimer:        make(chan bool),
	}
}

func (w *WsSession) addSubscriber(ch chan WsMessage) {
	w.subscribersMutex.Lock()
	defer w.subscribersMutex.Unlock()

	w.subscribers[ch] = struct{}{}
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
				err = fmt.Errorf("Coinbene failed to read from ws, err: %s", err)
				log.Println(err)
				time.Sleep(1000 * time.Millisecond)
				panic(err)
			}()
			return
		}

		if string(data) == "ping" {
			go func() {
				err := w.WriteMessage([]byte("pong"))
				if err != nil {
					log.Printf("Coinbene ws failed to send pong, err: %s", err)
				}
			}()
			continue
		}

		msg := WsMessage{}
		json.Unmarshal(data, &msg)
		if err != nil {
			log.Println("Coinbene websocket unmarhsal error", err)
			continue
		}

		if msg.Message != "" {
			log.Printf("Coinbene ws error message: %s", msg.Message)
		}

		// Check for subscribers
		w.subscribersMutex.Lock()
		for ch := range w.subscribers {
			go func(ch chan WsMessage) {
				ch <- msg
			}(ch)
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
				log.Printf("Coinbene ws failed to send ping, err: %s", err)
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
	expires := time.Now().AddDate(0, 0, 7) // Login expires after 1 week
	timestamp := expires.Format(time.RFC3339)
	data := timestamp + "GET/login"
	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))

	req := WsRequest{
		Op:   "login",
		Args: []interface{}{apiKey, timestamp, signature},
	}

	authErr := make(chan error)
	authSuccess := make(chan bool)
	wsMsgs := make(chan WsMessage)

	go func() {
		for msg := range wsMsgs {
			// Check for auth response
			if msg.Event == "login" {
				if msg.Success == true {
					authSuccess <- true
				} else {
					authErr <- fmt.Errorf("%s", msg.Message)
				}
			}
		}
	}()

	// Register subscriber
	w.addSubscriber(wsMsgs)
	defer w.removeSubscriber(wsMsgs)

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

type BookUpdate struct {
	Market   gateway.Market
	Snapshot bool
	Asks     []gateway.PriceLevel
	Bids     []gateway.PriceLevel
}

func (w *WsSession) SubscribeBook(market gateway.Market, ch chan BookUpdate, quit chan bool) error {
	topic := fmt.Sprintf("spot/orderBook.%s%s", market.Pair.Base, market.Pair.Quote)
	topicWithDepth := fmt.Sprintf("%s.100", topic)

	req := WsRequest{
		Op:   "subscribe",
		Args: []interface{}{topicWithDepth},
	}

	subErr := make(chan error)
	msgCh := make(chan WsMessage)

	go func() {
	mainLoop:
		for msg := range msgCh {
			if msg.Topic == topic {
				if subErr != nil { // Confirm order book received
					subErr <- nil
					subErr = nil
				}

				if msg.Data != nil {
					var data []WsBook
					err := json.Unmarshal(msg.Data, &data)
					if err != nil {
						log.Printf("Coinbene failed to unmarshal book update data, err: %s, data: %s", err, string(msg.Data))
						continue
					}

					book := data[0]
					asks := make([]gateway.PriceLevel, len(book.Asks))
					bids := make([]gateway.PriceLevel, len(book.Bids))

					for i, p := range book.Asks {
						err := parsePriceLevel(asks, i, p, market)
						if err != nil {
							log.Println(err)
							continue mainLoop
						}
					}
					for i, p := range book.Bids {
						err := parsePriceLevel(bids, i, p, market)
						if err != nil {
							log.Println(err)
							continue mainLoop
						}
					}

					var snapshot bool
					if msg.Action == "insert" {
						snapshot = true
					}

					// Dispatch book update
					ch <- BookUpdate{
						Market:   market,
						Snapshot: snapshot,
						Asks:     asks,
						Bids:     bids,
					}
				}
			} else if msg.Event == "error" && msg.Code == 10510 {
				subErr <- fmt.Errorf("failed to sub to book, err msg: %s", msg.Message)
			}
		}
	}()

	// Register subscriber
	w.addSubscriber(msgCh)

	// Quit, cleanup
	if quit != nil {
		go func() {
			<-quit

			req := WsRequest{
				Op:   "unsubscribe",
				Args: []interface{}{topic},
			}
			err := w.SendRequest(req)
			if err != nil {
				log.Printf("Failed to unsubscribe from topic %s, err: %s", topic, err)
			}

			w.removeSubscriber(msgCh)
			close(msgCh)
		}()
	}

	// Send request, and wait for response
	err := w.SendRequest(req)
	if err != nil {
		return err
	}

	// Wait for auth response
	select {
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timed out waiting for 30 seconds for book subscribe response")
	case err := <-subErr:
		return err
	}
}

type WsKeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (w *WsSession) SubscribeUserEvent(ch chan []WsKeyValue, quit chan bool) error {
	topic := "spot/userEvent"
	req := WsRequest{
		Op:   "subscribe",
		Args: []interface{}{topic},
	}

	subErr := make(chan error)
	msgCh := make(chan WsMessage)

	go func() {
		for msg := range msgCh {
			if msg.Topic == topic {
				if subErr != nil { // Confirm order book received
					subErr <- nil
					subErr = nil
				}

				if msg.Data != nil {
					var events []WsKeyValue
					err := json.Unmarshal(msg.Data, &events)
					if err != nil {
						log.Printf("Coinbene failed to unmarshal book update data, err: %s, data: %s", err, string(msg.Data))
						continue
					}

					ch <- events
				}
			} else if msg.Event == "error" && msg.Code == 10510 {
				subErr <- fmt.Errorf("failed to sub to user events, err msg: %s", msg.Message)
			}
		}
	}()

	// Register subscriber
	w.addSubscriber(msgCh)

	// Quit, cleanup
	if quit != nil {
		go func() {
			<-quit

			req := WsRequest{
				Op:   "unsubscribe",
				Args: []interface{}{topic},
			}
			err := w.SendRequest(req)
			if err != nil {
				log.Printf("Failed to unsubscribe from topic %s, err: %s", topic, err)
			}

			w.removeSubscriber(msgCh)
			close(msgCh)
		}()
	}

	// Send request, and wait for response
	err := w.SendRequest(req)
	if err != nil {
		return err
	}

	// Wait for auth response
	select {
	case <-time.After(15 * time.Second):
		return fmt.Errorf("timed out waiting for 15 seconds for book subscribe response")
	case err := <-subErr:
		return err
	}
}

func (w *WsSession) SubscribeUserOrderChange(ch chan bool, quit chan bool) error {
	userEventsCh := make(chan []WsKeyValue, 0)

	err := w.SubscribeUserEvent(userEventsCh, quit)
	if err != nil {
		return err
	}

	go func() {
		for events := range userEventsCh {
			if wsKeyValuesContains(events, []string{"curorder_changed", "hisorder_changed"}) {
				ch <- true
			}
		}
	}()

	return nil
}

func wsKeyValuesContains(events []WsKeyValue, keys []string) bool {
	for _, event := range events {
		for _, key := range keys {
			if event.Key == key {
				return true
			}
		}
	}

	return false
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

func parsePriceLevel(arr []gateway.PriceLevel, i int, data [2]string, market gateway.Market) error {
	px, err := strconv.ParseFloat(data[0], 64)
	if err != nil {
		err = fmt.Errorf("failed to parse float from book price, err: %s, market: %s, msg %+v", err, market.Symbol, err)
		return err
	}
	amount, err := strconv.ParseFloat(data[1], 64)
	if err != nil {
		err = fmt.Errorf("failed to parse float from book amount, err: %s, market: %s, msg %+v", err, market.Symbol, err)
		return err
	}
	arr[i] = gateway.PriceLevel{
		Price:  px,
		Amount: amount,
	}

	return nil
}
