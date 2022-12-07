package gemini

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/go-querystring/query"
	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

var wsUrl = "wss://api.gemini.com/v1/marketdata/:symbol"

type WsSession struct {
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	market           gateway.Market
	quit             chan bool
	subscribers      map[chan WsResponse]struct{}
	subscribersMutex *sync.Mutex
}

type WsRequest struct {
	Bids      bool `url:"bids"`
	Offers    bool `url:"offers"`
	Trades    bool `url:"trades"`
	Heartbeat bool `url:"heartbeat"`
}

type WsResponse struct {
	Type           string    `json:"type"`
	EventId        string    `json:"eventId"`
	ScoketSequence int64     `json:"scoketSequence"`
	Events         []WsEvent `json:"events"`
}

type WsEvent struct {
	Type  string  `json:"type"`
	Price float64 `json:"price,string"`

	// change event
	Side      string  `json:"side"`
	Remaining float64 `json:"remaining,string"`
	Delta     float64 `json:"delta,string"`
	Reason    string  `json:"reason"`

	// trade event
	TradeId   int64   `json:"tid"`
	Amount    float64 `json:"amount,string"`
	MakerSide string  `json:"makerSide"`
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

func NewWsSession(market gateway.Market) *WsSession {
	return &WsSession{
		market:           market,
		quit:             make(chan bool),
		subscribers:      make(map[chan WsResponse]struct{}),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
	}
}

func (s *WsSession) Connect(request WsRequest) error {
	data, _ := query.Values(request)
	encodedData := data.Encode()

	url := strings.Replace(wsUrl, ":symbol", s.market.Symbol, 1)
	url = fmt.Sprintf("%s?%s", url, encodedData)

	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(url, nil)
	if err != nil {
		return err
	}

	s.conn = ws

	go s.messageHandler()

	return nil
}

func (s *WsSession) messageHandler() {
	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				wsError := utils.NewWsError(
					Exchange.Name,
					utils.WsReadError,
					err.Error(),
					"",
				)

				panic(wsError.AsError())
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
