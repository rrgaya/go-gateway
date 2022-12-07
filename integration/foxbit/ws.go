package foxbit

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
	"golang.org/x/net/proxy"
)

var WS_URL = "wss://api.foxbit.com.br/"

const (
	OMSId = 1
)

type WsSession struct {
	options          gateway.Options
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	quit             chan bool
	subscribers      map[string]chan WsMessage
	subscribersMutex *sync.Mutex
	authenticated    bool
}

type WsMessage struct {
	Type           int    `json:"m"`
	SequenceNumber int64  `json:"i"`
	FunctionName   string `json:"n"`
	Payload        string `json:"o"`
}

type WsAuthenticateUser struct {
	ApiKey    string `json:"ApiKey"`
	Signature string `json:"Signature"`
	UserId    string `json:"UserId"`
	Nonce     string `json:"Nonce"`
	ApiSecret string `json:"-"`
}

func (a *WsAuthenticateUser) Sign() {
	signer := hmac.New(sha256.New, []byte(a.ApiSecret))
	signer.Write([]byte(fmt.Sprint(a.Nonce) + fmt.Sprint(a.UserId) + a.ApiKey))

	a.Signature = hex.EncodeToString(signer.Sum(nil))
}

type WsInstrument struct {
	InstrumentId      int     `json:"InstrumentId"`
	Symbol            string  `json:"Symbol"`
	SessionStatus     string  `json:"SessionStatus"`
	QuantityIncrement float64 `json:"QuantityIncrement"`
	PriceIncrement    float64 `json:"PriceIncrement"`
	MinimumQuantity   float64 `json:"MinimumQuantity"`
	MinimumPrice      float64 `json:"MinimumPrice"`
	IsDisabled        bool    `json:"IsDisabled"`
	Product1Symbol    string  `json:"Product1Symbol"`
	Product2Symbol    string  `json:"Product2Symbol"`
}

type WsOrderRequest struct {
	AccountID          int64   `json:"AccountId"`
	ClientOrderID      int64   `json:"ClientOrderId"`
	DisplayQuantity    float64 `json:"DisplayQuantity"`
	InstrumentID       int     `json:"InstrumentId"`
	LimitOffset        float64 `json:"LimitOffset"`
	LimitPrice         float64 `json:"LimitPrice"`
	OMSId              int64   `json:"OMSId"`
	OrderIDOCO         int64   `json:"OrderIdOCO"`
	OrderType          int64   `json:"OrderType"` // 1 Market | 2 Limit
	PegPriceType       int64   `json:"PegPriceType"`
	Quantity           float64 `json:"Quantity"`
	Side               int64   `json:"Side"` // 0 Buy | 1 Sell | 2 Short | 3 Unknown
	StopPrice          float64 `json:"StopPrice"`
	TimeInForce        int64   `json:"TimeInForce"`
	TrailingAmount     float64 `json:"TrailingAmount"`
	UseDisplayQuantity bool    `json:"UseDisplayQuantity"`
}

type WsOrder struct {
	Side             string  `json:"Side"`
	OrderId          int     `json:"OrderId"`
	Price            float64 `json:"Price"`
	Quantity         float64 `json:"Quantity"`
	QuantityExecuted float64 `json:"QuantityExecuted"`
	InstrumentId     int     `json:"InstrumentId"`
	AccountId        int64   `json:"AccountId"`
	OrderType        string  `json:"OrderType"`
	OrderState       string  `json:"OrderState"`
}

type WsBalance struct {
	AccountId     int64   `json:"AccountId"`
	ProductSymbol string  `json:"ProductSymbol"`
	ProductId     int64   `json:"ProductId"`
	Amount        float64 `json:"Amount"`
	Hold          float64 `json:"Hold"`
}

func NewWsSession(options gateway.Options) *WsSession {
	return &WsSession{
		options:          options,
		quit:             make(chan bool),
		subscribers:      make(map[string]chan WsMessage),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
		authenticated:    false,
	}
}

func (session *WsSession) Connect() error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	var socksURL *url.URL
	if len(session.options.Proxies) > 0 {
		randSource := rand.NewSource(time.Now().UnixNano())
		r := rand.New(randSource)
		n := r.Intn(len(session.options.Proxies))
		socksURL = session.options.Proxies[n]

		log.Printf("%s ws using proxy [%s] at rand index [%d]", Exchange, socksURL, n)
	}

	if socksURL != nil {
		proxyDialer, err := proxy.FromURL(socksURL, proxy.Direct)
		if err != nil {
			return fmt.Errorf("%s proxy from uri %s failed, err: %s", Exchange, socksURL, err)
		}

		dialer.NetDial = proxyDialer.Dial
	}

	ws, _, err := dialer.Dial(WS_URL, nil)
	if err != nil {
		return err
	}

	session.conn = ws

	go session.messageHandler()

	return nil
}

func (session *WsSession) messageHandler() {
	for {
		_, data, err := session.conn.ReadMessage()

		if err != nil {
			// Delay panicking, so we can finish sending message to subscribers
			go func() {
				time.Sleep(100 * time.Millisecond)
				panic(fmt.Errorf("Foxbit failed to read from ws. Err: %s", err))
			}()
			return
		}

		resp := WsMessage{}
		json.Unmarshal(data, &resp)

		if err != nil {
			log.Println("Foxbit websocket unmarhsal error", err)
			continue
		}

		// WS server error
		if resp.Type > 4 {
			log.Printf("Foxbit websocket error response. Err: %s", resp.Payload)
		}

		// Authentication is handled inside this method. Otherwise, we send
		// the message for the subscribers in order to be handler in the gateways.
		switch resp.FunctionName {
		case "AuthenticateUser":
			val, _, _, _ := jsonparser.Get([]byte(resp.Payload), "Authenticated")
			authenticated, _ := strconv.ParseBool(string(val))

			if authenticated == false {
				log.Printf("Foxbit failed to authenticate. Body: %s", resp.Payload)
			}

			session.authenticated = authenticated
		// We just receive one message in from each subscribe channel. They
		// can be removed after that.
		case "SubscribeLevel2", "SubscribeTrades":
			session.subscribersMutex.Lock()
			var targetChan chan WsMessage
			for functionName, ch := range session.subscribers {
				if functionName == resp.FunctionName {
					ch <- resp
					targetChan = ch
				}
			}
			session.subscribersMutex.Unlock()

			session.removeSubscriber(targetChan, resp.FunctionName)
		default:
			session.subscribersMutex.Lock()
			for functionName, ch := range session.subscribers {
				if functionName == resp.FunctionName {
					ch <- resp
				}
			}
			session.subscribersMutex.Unlock()
		}
	}
}

func (session *WsSession) SendRequest(request WsMessage) error {
	data, err := json.Marshal(request)

	if err != nil {
		return err
	}

	return session.WriteMessage(data)
}

func (session *WsSession) SendAuthRequest(request WsAuthenticateUser) error {
	data, err := json.Marshal(request)

	if err != nil {
		return err
	}

	return session.WriteMessage(data)
}

func (session *WsSession) WriteMessage(data []byte) error {
	session.connWriterMutex.Lock()
	err := session.conn.WriteMessage(websocket.TextMessage, data)
	session.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func (session *WsSession) RequestSubscriptions(requests []WsMessage) error {
	for _, req := range requests {
		err := session.SendRequest(req)
		if err != nil {
			return err
		}
	}

	return nil
}

func (session *WsSession) SubscribeMessages(functionName string, ch chan WsMessage, quit chan bool) {
	session.addSubscriber(functionName, ch)

	if quit != nil {
		go func() {
			<-quit
			session.removeSubscriber(ch, functionName)
		}()
	}
}

func (session *WsSession) addSubscriber(functionName string, ch chan WsMessage) {
	session.subscribersMutex.Lock()
	defer session.subscribersMutex.Unlock()

	session.subscribers[functionName] = ch
}

func (session *WsSession) removeSubscriber(chToRemove chan WsMessage, functionNameToRemove string) {
	session.subscribersMutex.Lock()
	defer session.subscribersMutex.Unlock()

	for functionName, ch := range session.subscribers {
		if chToRemove == ch && functionNameToRemove == functionName {
			delete(session.subscribers, functionName)
		}
	}
}
