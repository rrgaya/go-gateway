package digifinex

import (
	"bytes"
	"compress/zlib"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
	"golang.org/x/net/proxy"
)

var WsUrl = "wss://openapi.digifinex.com/ws/v1/"

type WsSession struct {
	options       gateway.Options
	conn          *websocket.Conn
	message       chan WsMessage
	onDisconnect  chan error
	requests      map[int64]chan WsMessage
	requestId     int64
	requestIdLock *sync.Mutex
	receivedRead  chan bool
	quit          chan bool
	socksURL      *url.URL
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		options:       options,
		message:       make(chan WsMessage),
		requests:      make(map[int64]chan WsMessage),
		quit:          make(chan bool),
		receivedRead:  make(chan bool),
		requestId:     100,
		requestIdLock: &sync.Mutex{},
	}
}

func (w *WsSession) SetOnDisconnect(onDisconnect chan error) {
	w.onDisconnect = onDisconnect
}

func (w *WsSession) SetProxy(uri *url.URL) {
	w.socksURL = uri
	return
}

type WsMessage struct {
	Method string          `json:"method"`
	Error  string          `json:"error"`
	Id     int64           `json:"id"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
}

type WsDepthUpdate struct {
	Asks []gateway.PriceArray `json:"asks"`
	Bids []gateway.PriceArray `json:"bids"`
}

type WsDeal struct {
	Id        int64   `json:"id"`
	Timestamp float64 `json:"time"`
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	Type      string  `json:"string"`
}

type WsOrder struct {
	Amount    float64 `json:"amount,string"`
	Filled    float64 `json:"filled,string"`
	ID        string  `json:"id"`
	Mode      int64   `json:"mode"`
	Notional  float64 `json:"notional,string"`
	Price     float64 `json:"price,string"`
	PriceAvg  float64 `json:"price_avg,string"`
	Side      string  `json:"side"`
	Status    int64   `json:"status"`
	Symbol    string  `json:"symbol"`
	Timestamp int64   `json:"timestamp"`
	Type      string  `json:"type"`
}

func NewWsRequest(method string, params interface{}) WsRequest {
	return WsRequest{
		Method: method,
		Params: params,
	}
}

type WsRequest struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	Id     int64       `json:"id"`
}

func (w *WsSession) Message() chan WsMessage {
	return w.message
}

func (w *WsSession) Connect() error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	if w.socksURL != nil {
		proxyDialer, err := proxy.FromURL(w.socksURL, proxy.Direct)
		if err != nil {
			return fmt.Errorf("DigiFinex proxy from uri %s failed, err: %s", w.socksURL, err)
		}

		dialer.NetDial = proxyDialer.Dial
	}

	ws, resp, err := dialer.Dial(WsUrl, nil)
	if err != nil {
		return fmt.Errorf("err: %s, resp status code: %s", err, resp.Status)
	}

	w.conn = ws

	go w.websocketPinger()
	go w.idleConnectionChecker()
	go w.messageHandler()

	return nil
}

func (w *WsSession) Close() {
	close(w.quit)
	w.conn.Close()
}

func (w *WsSession) messageHandler() {
	for {
		_, zMessage, err := w.conn.ReadMessage()
		if err != nil {
			if w.onDisconnect != nil {
				w.onDisconnect <- err
				return
			}
			panic(err)
		}

		w.receivedRead <- true

		msgReader := bytes.NewReader(zMessage)
		reader, err := zlib.NewReader(msgReader)
		if err != nil {
			log.Printf("Failed to initialize zlib reader, err %s", err)
			continue
		}

		message, err := ioutil.ReadAll(reader)
		if err != nil {
			log.Printf("Failed to read zlib, err %s", err)
			continue
		}

		wsMessage := WsMessage{}
		err = json.Unmarshal(message, &wsMessage)
		if err != nil {
			log.Println("DigiFinex websocket unmarhsal error", err)
			continue
		}

		// Check for request callbacks
		if wsMessage.Id > 0 {
			resCh, ok := w.requests[wsMessage.Id]
			if ok {
				delete(w.requests, wsMessage.Id)
				resCh <- wsMessage
			}
		}

		w.message <- wsMessage
	}
}

func (w *WsSession) idleConnectionChecker() {
	timeout := 15 * time.Second
	for {
		select {
		case _ = <-w.receivedRead:
		case <-time.After(timeout):
			err := fmt.Errorf("DigiFinex stale connection, haven't received any pong or message in %v", timeout)
			if w.options.Verbose {
				log.Printf("%s", err)
			}
			w.Close()
			w.onDisconnect <- err
			return
		}
	}
}

func (w *WsSession) websocketPinger() {
	for {
		select {
		case <-time.After(5 * time.Second):
			err := w.sendPing()
			if err != nil {
				err = fmt.Errorf("DigiFinex ws failed to send ping request, err: %s", err)
				if w.options.Verbose {
					log.Printf("%s", err)
				}
			}
		case <-w.quit:
			return
		}
	}
}

func (w *WsSession) SendRequest(message *WsRequest) (WsMessage, error) {
	return w.SendRequestWithTimeout(message, 5*time.Second)
}

func (w *WsSession) SendRequestWithTimeout(message *WsRequest, timeout time.Duration) (WsMessage, error) {
	resCh, err := w.SendRequestAsync(message)
	if err != nil {
		return WsMessage{}, err
	}

	select {
	case res := <-resCh:
		return res, nil
	case <-time.After(timeout):
		return WsMessage{}, errors.New(fmt.Sprintf("Request id %d timed out, didn't receive a response within %d seconds", message.Id, timeout/time.Second))
	}
}

func (w *WsSession) SendRequestAsync(message *WsRequest) (chan WsMessage, error) {
	msgChan := make(chan WsMessage)

	w.requestIdLock.Lock()
	w.requestId++
	message.Id = w.requestId
	w.requestIdLock.Unlock()

	// Register callback
	w.requests[message.Id] = msgChan

	err := w.SendMessage(message)
	if err != nil {
		delete(w.requests, message.Id) // Deregister
		close(msgChan)
		return nil, err
	}

	return msgChan, nil
}

func (w *WsSession) SendMessage(message *WsRequest) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return w.conn.WriteMessage(websocket.TextMessage, data)
}

func (w *WsSession) sendPing() error {
	pingReq := WsRequest{
		Method: "server.ping",
		Params: [0]int64{},
	}

	res, err := w.SendRequest(&pingReq)
	if err != nil {
		return err
	}

	if !strings.Contains(string(res.Result), "pong") {
		return fmt.Errorf("Ping request response failed, please inspect, response: %s", string(res.Result))
	}

	return nil
}

func (w *WsSession) Authenticate(apiKey, apiSecret string) error {
	timestampMilli := time.Now().UnixMilli()
	timestampStr := strconv.FormatInt(timestampMilli, 10)

	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(timestampStr))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	authReq := WsRequest{
		Method: "server.auth",
		Params: []interface{}{
			apiKey,
			timestampStr,
			signature,
		},
	}

	res, err := w.SendRequest(&authReq)
	if err != nil {
		panic(fmt.Errorf("failed to send auth: %s", err))
	}

	if !strings.Contains(string(res.Result), "success") {
		return fmt.Errorf("Auth failure response, please inspect, response: %s", string(res.Result))
	}

	return nil
}
