package hotcoin

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/utils"
)

var wsUrl = "wss://wss.hotcoinfin.com/trade/multiple"

type WsSession struct {
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[chan WsResponse]struct{}
	subscribersMutex *sync.Mutex
}

type WsRequest struct {
	Sub string `json:"sub"`
}

type WsResponse struct {
	Channel   string          `json:"ch"`
	Code      int             `json:"code"`
	Message   string          `json:"msg"`
	Status    string          `json:"status"`
	Timestamp int             `json:"ts"`
	Data      json.RawMessage `json:"data"`
}

func NewWsSession() *WsSession {
	return &WsSession{
		quit:             make(chan bool),
		subscribers:      make(map[chan WsResponse]struct{}),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
	}
}

func (s *WsSession) Connect() error {
	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(wsUrl, nil)
	if err != nil {
		return err
	}

	s.conn = ws

	go s.messageHandler()

	return nil
}

func (s *WsSession) messageHandler() {
	for {
		_, gzip, err := s.conn.ReadMessage()
		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				wsError := utils.NewWsError(
					"Hotcoin",
					utils.WsReadError,
					err.Error(),
					"",
				)

				panic(wsError.AsError())
			}()
			return
		}

		// TODO: handle error
		data := decompressGzip(gzip)
		_, dataType, _, _ := jsonparser.Get(data, "ping")

		// The mesage can be either {"ping": "ping"} or WsResponse.
		if dataType != jsonparser.NotExist {
			go func() {
				err := s.WriteMessage([]byte(`{"pong":"pong"}`))
				if err != nil {
					log.Printf("Pong to Hotcoin failed :%s", err)
				}
			}()
			continue
		}

		resp := WsResponse{}
		err = json.Unmarshal(data, &resp)
		if err != nil {
			wsError := utils.NewWsError(
				"Hotcoin",
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

func (ws *WsSession) WriteMessage(data []byte) error {
	ws.connWriterMutex.Lock()
	err := ws.conn.WriteMessage(websocket.TextMessage, data)
	ws.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func (ws *WsSession) SubscribeMessages(ch chan WsResponse, quit chan bool) {
	ws.addSubscriber(ch)

	if quit != nil {
		go func() {
			<-quit
			ws.removeSubscriber(ch)
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

func decompressGzip(data []byte) []byte {
	reader := bytes.NewReader([]byte(data))
	gzreader, err := gzip.NewReader(reader)

	if err != nil {
		wsError := utils.NewWsError(
			"Hotcoin",
			utils.WsReadError,
			err.Error(),
			"",
		)
		panic(wsError.AsError())
	}

	output, err := ioutil.ReadAll(gzreader)

	if err != nil {
		wsError := utils.NewWsError(
			"Hotcoin",
			utils.WsReadError,
			err.Error(),
			"",
		)
		panic(wsError.AsError())
	}

	return output
}
