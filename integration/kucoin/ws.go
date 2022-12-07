package kucoin

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/utils"
)

var WsUrl = "wss://ws-api.kucoin.com/endpoint"

type WsSession struct {
	api              *API
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsResponse]bool
	subscribersMutex *sync.Mutex
	onDisconnect     chan error
}

func NewWsSession(api *API) *WsSession {
	return &WsSession{
		quit:             make(chan bool),
		subscribers:      make(map[chan WsResponse]bool),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
		api:              api,
	}
}

func (w *WsSession) SetOnDisconnect(onDisconnect chan error) {
	w.onDisconnect = onDisconnect
}

type WsRequest struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Topic          string `json:"topic"`
	Response       bool   `json:"response"`
	PrivateChannel bool   `json:"privateChannel"`
}

type WsResponse struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Topic   string          `json:"topic"`
	Subject string          `json:"subject"`
	Data    json.RawMessage `json:"data"`
}

type WsOrderBookHeader struct {
	SequenceStart int64       `json:"sequenceStart"`
	SequenceEnd   int64       `json:"sequenceEnd"`
	Symbol        string      `json:"symbol"`
	Changes       WsOrderBook `json:"changes"`
}

type WsOrderBook struct {
	Asks []OrderBookEntry `json:"asks"`
	Bids []OrderBookEntry `json:"bids"`
}

type WsOrder struct {
	ClientOid  string  `json:"clientOid"`
	FilledSize float64 `json:"filledSize,string"`
	Liquidity  string  `json:"liquidity"`
	MatchPrice float64 `json:"matchPrice,string"`
	MatchSize  float64 `json:"matchSize,string"`
	OrderID    string  `json:"orderId"`
	OrderTime  int64   `json:"orderTime"`
	OrderType  string  `json:"orderType"`
	Price      float64 `json:"price,string"`
	RemainSize float64 `json:"remainSize,string"`
	Side       string  `json:"side"`
	Size       float64 `json:"size,string"`
	Status     string  `json:"status"`
	Symbol     string  `json:"symbol"`
	TradeID    string  `json:"tradeId"`
	Ts         int64   `json:"ts"`
	Type       string  `json:"type"`
}

type OrderBookEntry struct {
	Price    float64
	Size     float64
	Sequence int64
}

func (o *OrderBookEntry) UnmarshalJSON(b []byte) error {
	var data [3]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}

	o.Price, _ = strconv.ParseFloat(data[0].(string), 64)
	o.Size, _ = strconv.ParseFloat(data[1].(string), 64)
	o.Sequence, _ = strconv.ParseInt(data[2].(string), 10, 64)

	return nil
}

func (s *WsSession) Connect(private bool) error {
	bullet, err := s.api.GetBullet(private)
	if err != nil {
		return fmt.Errorf("get bullet err: %s", err)
	}

	if len(bullet.InstanceServers) == 0 {
		return fmt.Errorf("get bullet didn't return any instance servers to connect to")
	}

	instanceServer := bullet.InstanceServers[0]

	log.Printf("WebSocket connecting to url [%s] private [%v]", instanceServer.Endpoint, private)

	url := fmt.Sprintf("%s?token=%s", instanceServer.Endpoint, bullet.Token)
	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(url, nil)
	if err != nil {
		return err
	}

	s.conn = ws

	go s.websocketPinger()
	go s.messageHandler()

	return nil
}

func (s *WsSession) messageHandler() {
	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			if s.onDisconnect != nil {
				s.onDisconnect <- err
				return
			}
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				panic(fmt.Errorf("%s failed to read from ws, err: %s", Exchange.Name, err))
			}()
			return
		}

		resp := WsResponse{}
		json.Unmarshal(data, &resp)
		if err != nil {
			wsError := utils.NewWsError(
				Exchange.Name,
				utils.WsUnmarshalError,
				err.Error(),
				string(data),
			)
			log.Println(wsError.AsError())
			continue
		}

		s.subscribersMutex.Lock()
		for ch := range s.subscribers {
			ch <- resp
		}
		s.subscribersMutex.Unlock()
	}
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

	s.subscribers[ch] = true
}

func (s *WsSession) removeSubscriber(ch chan WsResponse) {
	s.subscribersMutex.Lock()
	defer s.subscribersMutex.Unlock()

	delete(s.subscribers, ch)
}

func (w *WsSession) websocketPinger() {
	for {
		select {
		case <-time.After(10 * time.Second):
			err := w.sendPing()
			if err != nil {
				log.Printf("%s ws failed to send ping, err: %s", Exchange.Name, err)
			}
		case <-w.quit:
			return
		}
	}
}

func (w *WsSession) sendPing() error {
	w.connWriterMutex.Lock()
	err := w.conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("{\"id\":\"%d\", \"type\":\"ping\"}", time.Now().UnixMilli())))
	w.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}
