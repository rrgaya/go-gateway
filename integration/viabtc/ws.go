package viabtc

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
	"golang.org/x/net/proxy"
)

type WsSession struct {
	exchange      gateway.Exchange
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

func NewWsSession(exchange gateway.Exchange, options gateway.Options, onDisconnect chan error) *WsSession {
	return &WsSession{
		exchange:      exchange,
		options:       options,
		message:       make(chan WsMessage),
		requests:      make(map[int64]chan WsMessage),
		quit:          make(chan bool),
		receivedRead:  make(chan bool),
		onDisconnect:  onDisconnect,
		requestId:     100,
		requestIdLock: &sync.Mutex{},
	}
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

type DepthUpdate struct {
	Asks []gateway.PriceArray `json:"asks"`
	Bids []gateway.PriceArray `json:"bids"`
}

type Deal struct {
	Id        int64   `json:"id"`
	Timestamp float64 `json:"time"`
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	Type      string  `json:"string"`
}

func (d *Deal) Time() time.Time {
	sec, dec := math.Modf(d.Timestamp)
	return time.Unix(int64(sec), int64(dec*(1e9)))
}

func NewDepthSubscribeParams(symbol string, maxDepth int64, priceTick float64) [3]interface{} {
	var params [3]interface{}

	params[0] = symbol
	params[1] = maxDepth
	params[2] = strconv.FormatFloat(priceTick, 'f', -1, 64)

	return params
}

func (w *WsSession) Message() chan WsMessage {
	return w.message
}

func (w *WsSession) Connect(uri, origin string) error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	if w.socksURL != nil {
		proxyDialer, err := proxy.FromURL(w.socksURL, proxy.Direct)
		if err != nil {
			return fmt.Errorf("%s proxy from uri %s failed, err: %s", w.exchange, w.socksURL, err)
		}

		dialer.NetDial = proxyDialer.Dial
	}

	headers := http.Header{}
	headers.Add("User-Agent", w.options.UserAgent)
	headers.Add("Cookie", w.options.Cookie)
	headers.Add("Origin", origin)

	ws, resp, err := dialer.Dial(uri, headers)
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
		dataType, data, err := w.conn.ReadMessage()
		if err != nil {
			if w.onDisconnect != nil {
				w.onDisconnect <- err
				return
			}
			panic(fmt.Errorf("read msg err: %s", err))
		}

		w.receivedRead <- true

		if dataType == websocket.BinaryMessage {
			msgReader := bytes.NewReader(data)
			reader, err := gzip.NewReader(msgReader)
			if err != nil {
				log.Printf("Failed to initialize gzip reader, err %s", err)
				continue
			}

			decompressedData, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Printf("Failed to read zlib, err %s", err)
				continue
			}

			data = decompressedData
		}

		wsMessage := WsMessage{}
		err = json.Unmarshal(data, &wsMessage)
		if err != nil {
			log.Printf("%s websocket unmarhsal error", w.exchange, err)
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
			err := fmt.Errorf("%s stale connection, haven't received any pong or message in %v", w.exchange, timeout)
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
				err = fmt.Errorf("%s ws failed to send ping request, err: %s", w.exchange, err)
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
		return errors.New(fmt.Sprintf("Ping request response failed, please inspect, response: %s", string(res.Result)))
	}

	return nil
}
