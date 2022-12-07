package bitforex

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

const (
	WsBase             = "wss://www.bitforex.com"
	WsOrdersApi        = "/ordersapi/ws"
	WsMktApiCoinGroup1 = "/mkapi/coinGroup1/ws"
)

type WsSession struct {
	options         gateway.Options
	url             string
	conn            *websocket.Conn
	connWriterMutex *sync.Mutex
	wsMessageCh     chan WsMessage
	lastPong        time.Time
	quit            chan bool
}

type WsRequest struct {
	Type  string          `json:"type"`
	Event string          `json:"event"`
	Param json.RawMessage `json:"param"`
}

type WsMessage struct {
	Type    string          `json:"type"`
	Event   string          `json:"event"`
	Data    json.RawMessage `json:"data"`
	Param   json.RawMessage `json:"param"`
	Success bool            `json:"success"`
	Code    string          `json:"code"`
}

type WsDepthSubParam struct {
	BusinessType string `json:"businessType"`
	DType        int64  `json:"dType"`
	Size         int64  `json:"size"`
}

type WsAuthParam struct {
	UID   int64  `json:"uid"`
	Token string `json:"token"`
}

type WsOrderSubParam struct {
	BusinessType string `json:"businessType"`
}

type WsDepthUpdate struct {
	Asks []gateway.PriceLevel
	Bids []gateway.PriceLevel
}

func NewWsSession(options gateway.Options, wsMessageCh chan WsMessage) *WsSession {
	return &WsSession{
		options:     options,
		wsMessageCh: wsMessageCh,
		quit:        make(chan bool),
	}
}

func NewWsRequest(_type, event string, params interface{}) (WsRequest, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return WsRequest{}, err
	}

	return WsRequest{
		Type:  _type,
		Event: event,
		Param: data,
	}, nil
}

func (w *WsSession) Connect(path string) error {
	dialer := websocket.Dialer{}

	headers := http.Header{}

	if string(path[0]) != "/" {
		path = "/" + path
	}

	w.url = WsBase + path
	ws, _, err := dialer.Dial(w.url, headers)
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
	log.Printf("Bitforex closing ws...")

	close(w.quit)

	w.conn.Close()
}

func (w *WsSession) messageHandler() {
	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			panic(fmt.Errorf("Bitforex ws %s failed to read, err: %s", w.url, err))
		}

		//log.Printf("rcv %s", string(message))
		wsMessage := WsMessage{}
		json.Unmarshal(message, &wsMessage)
		if err != nil {
			log.Println("Bitforex websocket unmarhsal error", err)
			continue
		}

		w.wsMessageCh <- wsMessage
	}
}

func (w *WsSession) websocketPinger() {
	for {
		select {
		case <-time.After(10 * time.Second):
			err := w.sendPing()
			if err != nil {
				log.Printf("Bitforex ws failed to send ping, err: %s", err)
			}
		case <-w.quit:
			return
		}
	}
}

func (w *WsSession) SendRequest(request WsRequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}

	return w.sendRequestData(data)
}

func (w *WsSession) SendRequests(request []WsRequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}

	return w.sendRequestData(data)
}

func (w *WsSession) sendRequestData(data []byte) error {
	w.connWriterMutex.Lock()
	err := w.conn.WriteMessage(websocket.TextMessage, data)
	w.connWriterMutex.Unlock()
	if err != nil {
		return err
	}
	//log.Printf("sent %s", string(data))

	return nil
}

func (w *WsSession) sendPing() error {
	w.connWriterMutex.Lock()
	err := w.conn.WriteMessage(websocket.TextMessage, []byte("ping_p"))
	w.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}
