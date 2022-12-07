package bitmart

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

var WsPublicURL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
var WsPrivateURL = "wss://ws-manager-compress.bitmart.com/user?protocol=1.1"

type WsSession struct {
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	pongRcvd         chan bool
	subscribers      map[chan WsResponse]bool
	subscribersMutex *sync.Mutex
}

type WsRequest struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
}

type WsResponse struct {
	Event string          `json:"event"`
	Table string          `json:"table"`
	Data  json.RawMessage `json:"data"`
}

type WsError struct {
	Event        string `json:"event"`
	ErrorMessage string `json:"errorMessage"`
	ErrorCode    string `json:"errorCode"`
}

type WsOrderBook struct {
	Symbol    string               `json:"symbol"`
	Asks      []gateway.PriceArray `json:"asks"`
	Bids      []gateway.PriceArray `json:"bids"`
	Timestamp int64                `json:"ms_t"`
}

type WsTrade struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price,string"`
	Side      string  `json:"side"`
	Size      float64 `json:"size,string"`
	Timestamp int64   `json:"s_t"`
}

type WsOrder struct {
	FilledNotional float64 `json:"filled_notional,string"`
	FilledSize     float64 `json:"filled_size,string"`
	OrderID        string  `json:"order_id"`
	OrderType      string  `json:"order_type"`
	Price          float64 `json:"price,string"`
	Size           float64 `json:"size,string"`
	Side           string  `json:"side"`
	State          string  `json:"state"`
	Symbol         string  `json:"symbol"`
	Type           string  `json:"type"`
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		quit:             make(chan bool),
		pongRcvd:         make(chan bool),
		subscribers:      make(map[chan WsResponse]bool),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
	}
}

func (s *WsSession) Connect(url string) error {
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
		dataType, data, err := s.conn.ReadMessage()
		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				panic(fmt.Errorf("%s failed to read from ws, err: %s", Exchange.Name, err))
			}()
			return
		}

		// Check if needs decompression
		if dataType == websocket.BinaryMessage {
			msgReader := bytes.NewReader(data)
			reader := flate.NewReader(msgReader)

			decompressedData, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Printf("Failed to read zlib, err %s", err)
				continue
			}

			reader.Close()
			data = decompressedData
		}

		errorMessage, _, _, _ := jsonparser.Get(data, "errorMessage")

		if string(errorMessage) != "" {
			errMsg := WsError{}

			err := json.Unmarshal(data, &errMsg)
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

			wsError := utils.NewWsError(
				Exchange.Name,
				utils.WsRequestError,
				fmt.Sprintf("%s - %s", errMsg.ErrorCode, errMsg.ErrorMessage),
				string(data),
			)
			fmt.Println(wsError.AsError())
			continue
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

		// Trigger pong
		select {
		case s.pongRcvd <- true:
		default:
		}
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
		case <-w.pongRcvd:
			// Reset ping timer
		case <-time.After(1 * time.Second):
			err := w.sendPing()
			if err != nil {
				panic(fmt.Errorf("%s ws failed to send ping, err: %s", Exchange.Name, err))
			}

			// Check for pong response
			select {
			case <-w.pongRcvd:
			case <-time.After(3 * time.Second):
				panic(fmt.Errorf("%s pong not received after 3 seconds...", Exchange.Name))
			}
		case <-w.quit:
			return
		}
	}
}

func (w *WsSession) sendPing() error {
	w.connWriterMutex.Lock()
	err := w.conn.WriteMessage(websocket.TextMessage, []byte("ping"))
	w.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}
