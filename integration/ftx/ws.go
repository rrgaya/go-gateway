package ftx

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

// https://docs.ftx.com/?python#request-process
var GateURL = "wss://ftx.com/ws/"

type WsSession struct {
	options          gateway.Options
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsResponse]struct{}
	subscribersMutex *sync.Mutex
}

type WsRequest struct {
	Channel string `json:"channel"`
	Market  string `json:"market"`
	Op      string `json:"op"`
	Args    struct {
		Key  string `json:"key"`
		Sign string `json:"sign"`
		Time int64  `json:"time"`
	} `json:"args,omitempty"`
}

type WsResponse struct {
	Channel string          `json:"channel"`
	Market  string          `json:"market"`
	Type    string          `json:"type"`
	Code    string          `json:"code,omitempty"`
	Msg     string          `json:"msg,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type WsTrade struct {
	ID          int64     `json:"id"`
	Price       float64   `json:"price"`
	Size        float64   `json:"size"`
	Side        string    `json:"side"`
	Liquidation bool      `json:"liquidation"`
	Time        time.Time `json:"time"`
}

type WsOrderBook struct {
	Action   string               `json:"action"`
	Bids     []gateway.PriceArray `json:"bids"`
	Asks     []gateway.PriceArray `json:"asks"`
	Checksum int64                `json:"checksum"`
	Time     float64              `json:"time"`
}

type WsOrder struct {
	ID            int64   `json:"id"`
	ClientID      int     `json:"clientId"`
	Market        string  `json:"market"`
	Type          string  `json:"type"`
	Side          string  `json:"side"`
	Size          float64 `json:"size"`
	Price         float64 `json:"price"`
	ReduceOnly    bool    `json:"reduceOnly"`
	Ioc           bool    `json:"ioc"`
	PostOnly      bool    `json:"postOnly"`
	Status        string  `json:"status"`
	FilledSize    float64 `json:"filledSize"`
	RemainingSize float64 `json:"remainingSize"`
	AvgFillPrice  float64 `json:"avgFillPrice"`
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		options:          options,
		quit:             make(chan bool),
		subscribers:      make(map[chan WsResponse]struct{}),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
	}
}

func (s *WsSession) Connect() error {
	dialer := websocket.Dialer{}

	ws, _, err := dialer.Dial(GateURL, nil)
	if err != nil {
		return err
	}

	s.conn = ws

	go s.websocketPinger()
	go s.messageHandler()

	return nil
}

func (s *WsSession) websocketPinger() {
	sendPing := func() error {
		pingReq := `{"op": "ping"}`

		err := s.conn.WriteMessage(websocket.TextMessage, []byte(pingReq))

		return err
	}

	for {
		select {
		case <-time.After(14 * time.Second):
			err := sendPing()
			if err != nil {
				err = fmt.Errorf("FTX ws failed to send ping request. Err: %s", err)
				if s.options.Verbose {
					log.Printf("FTX ws failed to sendPing. Err: %s", err)
				}
			}
		case <-s.quit:
			return
		}
	}
}

func (s *WsSession) messageHandler() {
	for {
		_, data, err := s.conn.ReadMessage()

		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				panic(fmt.Errorf("FTX failed to read from ws, err: %s", err))
			}()
			return
		}

		resp := WsResponse{}
		json.Unmarshal(data, &resp)
		if err != nil {
			log.Println("FTX websocket unmarhsal error", err)
			continue
		}

		if resp.Type == "error" {
			log.Printf("FTX websocket error response. Err: %v - %s", resp.Code, resp.Msg)
		}

		s.subscribersMutex.Lock()
		for ch := range s.subscribers {
			ch <- resp
		}
		s.subscribersMutex.Unlock()
	}
}

func (s *WsSession) Sign() string {
	mac := hmac.New(sha256.New, []byte(s.options.ApiSecret))
	time := strconv.FormatInt(time.Now().UnixMilli(), 10)
	payload := time + "websocket_login"
	mac.Write([]byte(payload))

	return hex.EncodeToString(mac.Sum(nil))
}

func (s *WsSession) SendRequest(request WsRequest) error {
	data, err := json.Marshal(request)

	if err != nil {
		return err
	}

	return s.WriteMessage(data)
}

func (s *WsSession) WriteMessage(data []byte) error {
	s.connWriterMutex.Lock()
	err := s.conn.WriteMessage(websocket.TextMessage, data)
	s.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func (s *WsSession) RequestSubscriptions(requests []WsRequest) error {
	for _, req := range requests {
		err := s.SendRequest(req)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *WsSession) SubscribeMessages(ch chan WsResponse, quit chan bool) {
	s.addSubscriber(ch)

	if quit != nil {
		go func() {
			<-quit
			s.removeSubscriber(ch)
		}()
	}
}

func (s *WsSession) addSubscriber(ch chan WsResponse) {
	s.subscribersMutex.Lock()
	defer s.subscribersMutex.Unlock()

	s.subscribers[ch] = struct{}{}
}

func (s *WsSession) removeSubscriber(ch chan WsResponse) {
	s.subscribersMutex.Lock()
	defer s.subscribersMutex.Unlock()

	delete(s.subscribers, ch)
}
