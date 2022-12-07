package bigone

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/golang-jwt/jwt"
	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
	"golang.org/x/net/proxy"
)

var WsUrl = "wss://big.one/ws/v2"

type WsSession struct {
	options       gateway.Options
	conn          *websocket.Conn
	onDisconnect  chan error
	requests      map[string]chan []byte
	requestId     int64
	requestIdLock *sync.Mutex
	lockWrite     *sync.Mutex
	quit          chan bool
	socksURL      *url.URL
}

func NewWsSession(options gateway.Options, onDisconnect chan error) *WsSession {
	return &WsSession{
		options:       options,
		requests:      make(map[string]chan []byte),
		quit:          make(chan bool),
		onDisconnect:  onDisconnect,
		requestId:     0,
		requestIdLock: &sync.Mutex{},
		lockWrite:     &sync.Mutex{},
	}
}

func (w *WsSession) SetProxy(uri *url.URL) {
	w.socksURL = uri
	return
}

type WsMessage struct {
	RequestID string `json:"requestId"`
}

type WsDepthSnapshot struct {
	Depth WsDepth `json:"depth"`
}

type WsDepth struct {
	Market string         `json:"market"`
	Asks   []WsPriceLevel `json:"asks"`
	Bids   []WsPriceLevel `json:"bids"`
}

type WsPriceLevel struct {
	Amount float64 `json:"amount,string"`
	Price  float64 `json:"price,string"`
}

type WsOrder struct {
	Amount       float64 `json:"amount,string"`
	AvgDealPrice float64 `json:"avgDealPrice,string"`
	CreatedAt    string  `json:"createdAt"`
	FilledAmount float64 `json:"filledAmount,string"`
	FilledFees   float64 `json:"filledFees,string"`
	ID           string  `json:"id"`
	Market       string  `json:"market"`
	Price        float64 `json:"price,string"`
	Side         string  `json:"side"`
	State        string  `json:"state"`
	StopPrice    string  `json:"stopPrice"`
	UpdatedAt    string  `json:"updatedAt"`
}

type WsMarketRequest struct {
	Market string `json:"market"`
}

type WsAuthRequest struct {
	Token string `json:"token"`
}

type WsRequest struct {
	RequestId                    string           `json:"requestId"`
	SubscribeViewerOrdersRequest *WsMarketRequest `json:"subscribeViewerOrdersRequest,omitempty"`
	SubscribeMarketDepthRequest  *WsMarketRequest `json:"subscribeMarketDepthRequest,omitempty"`
	AuthenticateCustomerRequest  *WsAuthRequest   `json:"authenticateCustomerRequest,omitempty"`
}

func (w *WsSession) Connect() error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	if w.socksURL != nil {
		proxyDialer, err := proxy.FromURL(w.socksURL, proxy.Direct)
		if err != nil {
			return fmt.Errorf("BigONE proxy from uri %s failed, err: %s", w.socksURL, err)
		}

		dialer.NetDial = proxyDialer.Dial
	}

	headers := http.Header{
		"Sec-WebSocket-Protocol": []string{"json"},
	}

	ws, resp, err := dialer.Dial(WsUrl, headers)
	if err != nil {
		return fmt.Errorf("err: %s, resp status code: %s", err, resp.Status)
	}

	w.conn = ws

	go w.messageHandler()

	return nil
}

func (w *WsSession) Close() {
	close(w.quit)
	w.conn.Close()
}

func (w *WsSession) messageHandler() {
	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			if w.onDisconnect != nil {
				w.onDisconnect <- err
				return
			}
			panic(err)
		}

		wsMessage := WsMessage{}
		err = json.Unmarshal(message, &wsMessage)
		if err != nil {
			log.Println("BigONE websocket unmarhsal error", err)
			continue
		}

		// Check for request callbacks
		if wsMessage.RequestID != "" {
			resCh, ok := w.requests[wsMessage.RequestID]
			if ok {
				resCh <- message
			}
		}
	}
}

func (w *WsSession) SendRequest(message *WsRequest) ([]byte, error) {
	return w.SendRequestWithTimeout(message, 15*time.Second)
}

func (w *WsSession) SendRequestWithTimeout(message *WsRequest, timeout time.Duration) ([]byte, error) {
	resCh, err := w.SendRequestAsync(message)
	if err != nil {
		return []byte{}, err
	}

	select {
	case res := <-resCh:
		return res, nil
	case <-time.After(timeout):
		return []byte{}, errors.New(fmt.Sprintf("Request id %s timed out, didn't receive a response within %d seconds", message.RequestId, timeout/time.Second))
	}
}

func (w *WsSession) SendRequestAsync(message *WsRequest) (chan []byte, error) {
	msgChan := make(chan []byte)

	w.requestIdLock.Lock()
	w.requestId++
	requestId := strconv.FormatInt(w.requestId, 10)
	message.RequestId = requestId
	w.requestIdLock.Unlock()

	// Register callback
	w.requests[requestId] = msgChan

	err := w.SendMessage(message)
	if err != nil {
		delete(w.requests, requestId) // Deregister
		close(msgChan)
		return nil, err
	}

	return msgChan, nil
}

func (w *WsSession) SendMessage(message *WsRequest) error {
	w.lockWrite.Lock()
	defer w.lockWrite.Unlock()

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return w.conn.WriteMessage(websocket.TextMessage, data)
}

func (w *WsSession) Authenticate() error {
	var signKey = []byte(w.options.ApiSecret)
	token := jwt.New(jwt.SigningMethodHS256)
	claims := token.Claims.(jwt.MapClaims)

	claims["type"] = "OpenAPIV2"
	claims["sub"] = w.options.ApiKey
	claims["nonce"] = strconv.FormatInt(time.Now().UnixNano(), 10)

	tokenStr, err := token.SignedString(signKey)
	if err != nil {
		return fmt.Errorf("jwt token gen err: %s", err)
	}

	authReq := WsRequest{
		AuthenticateCustomerRequest: &WsAuthRequest{
			Token: fmt.Sprintf("Bearer %s", tokenStr),
		},
	}

	res, err := w.SendRequest(&authReq)
	if err != nil {
		return fmt.Errorf("failed to send auth: %s", err)
	}

	errVal, dataType, _, _ := jsonparser.Get(res, "error")
	if dataType != jsonparser.NotExist {
		return fmt.Errorf("api responded with error message: %s\nresponse: %s", string(errVal), string(res))
	}

	return nil
}
