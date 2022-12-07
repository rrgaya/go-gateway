package latoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/go-stomp/stomp"
	"github.com/herenow/atomic-gtw/gateway"
	"golang.org/x/net/websocket"
)

var WsUrl = "wss://api.latoken.com/stomp"

const (
	OrderPlaced    string = "ORDER_STATUS_PLACED"
	OrderClosed    string = "ORDER_STATUS_CLOSED"
	OrderCancelled string = "ORDER_STATUS_CANCELLED"
	OrderRejected  string = "ORDER_STATUS_REJECTED"
)

type DepthUpdate struct {
	Market gateway.Market
	Bids   []WsPriceLevel
	Asks   []WsPriceLevel
}

type OrderUpdate struct {
	Market gateway.Market
	Order  WsOrder
}

type WsOrder struct {
	ID            string  `json:"id"`
	BaseCurrency  string  `json:"baseCurrency"`
	QuoteCurrency string  `json:"quoteCurrency"`
	ChangeType    string  `json:"changeType"`
	ClientOrderID string  `json:"clientOrderId"`
	Condition     string  `json:"condition"`
	Cost          float64 `json:"cost,string"`
	DeltaFilled   float64 `json:"deltaFilled,string"`
	Filled        float64 `json:"filled,string"`
	Price         float64 `json:"price,string"`
	Quantity      float64 `json:"quantity,string"`
	Side          string  `json:"side"`
	Status        string  `json:"status"`
	Timestamp     int64   `json:"timestamp"`
	Type          string  `json:"type"`
	User          string  `json:"user"`
}

type WsCurrency struct {
	Decimals    int64  `json:"decimals"`
	Description string `json:"description"`
	ID          string `json:"id"`
	Logo        string `json:"logo"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Tag         string `json:"tag"`
	Type        string `json:"type"`
}

type WsPair struct {
	ID                  string  `json:"id"`
	BaseCurrency        string  `json:"baseCurrency"`
	QuoteCurrency       string  `json:"quoteCurrency"`
	Status              string  `json:"status"`
	CostDisplayDecimals int64   `json:"costDisplayDecimals"`
	PriceTick           float64 `json:"priceTick,string"`
	QuantityTick        float64 `json:"quantityTick,string"`
}

type WsPriceLevel struct {
	Price          float64 `json:"price,string"`
	Quantity       float64 `json:"quantity,string"`
	Cost           float64 `json:"cost,string"`
	CostChange     float64 `json:"costChange,string"`
	QuantityChange float64 `json:"quantityChange,string"`
}

type WsSession struct {
	options         gateway.Options
	userID          string
	orderUpdateCh   chan OrderUpdate
	depthUpdateCh   chan DepthUpdate
	ordersState     map[string]WsOrder
	orderStateMutex *sync.RWMutex
	symbolsMap      map[string]gateway.Market
	marketsMap      map[gateway.Market]APIPair
	currencies      []APICurrency
}

func NewWsSession(options gateway.Options, userID string, orderUpdateCh chan OrderUpdate, depthUpdateCh chan DepthUpdate, symbolsMap map[string]gateway.Market, marketsMap map[gateway.Market]APIPair, currencies []APICurrency) *WsSession {
	return &WsSession{
		options:         options,
		userID:          userID,
		orderUpdateCh:   orderUpdateCh,
		depthUpdateCh:   depthUpdateCh,
		ordersState:     make(map[string]WsOrder),
		orderStateMutex: &sync.RWMutex{},
		symbolsMap:      symbolsMap,
		marketsMap:      marketsMap,
		currencies:      currencies,
	}
}

func (w *WsSession) getStompConn() (*stomp.Conn, error) {
	headers := http.Header{}
	if w.options.UserAgent != "" {
		headers.Add("User-Agent", w.options.UserAgent)
	}
	if w.options.Cookie != "" {
		headers.Add("Cookie", w.options.Cookie)
	}
	headers.Add("Host", "api.latoken.com")

	// Authentication
	if w.options.ApiKey != "" {
		expiresMs := time.Now().UnixNano() / int64(time.Millisecond)
		signData := strconv.FormatInt(expiresMs, 10)

		h := hmac.New(sha256.New, []byte(w.options.ApiSecret))
		h.Write([]byte(signData))
		signature := hex.EncodeToString(h.Sum(nil))

		headers.Set("X-LA-APIKEY", w.options.ApiKey)
		headers.Set("X-LA-SIGNATURE", signature)
		headers.Set("X-LA-DIGEST", "HMAC_SHA256")
		headers.Set("X-LA-SIGDATA", signData)
	}

	url, err := url.Parse(WsUrl)
	if err != nil {
		return nil, err
	}

	originUrl, _ := url.Parse("https://exchange.latoken.com")

	config := &websocket.Config{
		TlsConfig: &tls.Config{InsecureSkipVerify: true},
		Protocol:  []string{"v10.stomp", "v11.stomp", "v12.stomp"},
		Location:  url,
		Origin:    originUrl,
		Header:    headers,
		Version:   13,
	}

	conn, err := websocket.DialConfig(config)
	if err != nil {
		return nil, err
	}

	stompConn, err := stomp.Connect(
		conn,
		stomp.ConnOpt.HeartBeat(10*time.Second, 10*time.Second),
		stomp.ConnOpt.HeartBeatError(30*time.Second),
		stomp.ConnOpt.AcceptVersion(stomp.V10, stomp.V11, stomp.V12),
	)
	if err != nil {
		log.Printf("failed to connect to stomp err %v", err)
		return nil, err
	}

	return stompConn, nil
}

func (w *WsSession) Connect() error {
	stompConn, err := w.getStompConn()
	if err != nil {
		return err
	}

	log.Printf("LAToken Connected Stomp Server Version %v", stompConn.Version())

	// Subscribe to user feed
	if w.userID != "" {
		err = w.connectUserErrors(stompConn)
		if err != nil {
			return err
		}

		err = w.connectOrderUpdate(stompConn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *WsSession) connectUserErrors(stompConn *stomp.Conn) error {
	dest := fmt.Sprintf("/user/%s/v1/error", w.userID)

	log.Printf("LAToken subscribing to %s", dest)

	sub, err := stompConn.Subscribe(dest, stomp.AckAuto)
	if err != nil {
		return err
	}

	go func() {
		for msg := range sub.C {
			log.Printf("LAToken user received error msg body [%s]", string(msg.Body))
		}
	}()

	return nil
}

func (w *WsSession) connectOrderUpdate(stompConn *stomp.Conn) error {
	dest := fmt.Sprintf("/user/%s/v1/order", w.userID)

	log.Printf("LAToken subscribing to %s", dest)

	sub, err := stompConn.Subscribe(dest, stomp.AckAuto)
	if err != nil {
		return err
	}

	select {
	case msg := <-sub.C:
		if msg.Err != nil {
			return fmt.Errorf("Initial order update msg container err: %s", msg.Err)
		}
		log.Printf("LAToken received initial user order update")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("Timed out while waiting to receive initial user order update")
	}

	go func() {
		for msg := range sub.C {
			if msg.Err != nil {
				err = fmt.Errorf("Failed to receive user order update, err %s", msg.Err)
				log.Println(err)
				panic(err)
			}

			data, _, _, err := jsonparser.Get(msg.Body, "payload")
			if err != nil {
				log.Printf("Failed to json parse order update data msg %+v, err %v", msg, err)
				continue
			}

			var orders []WsOrder
			err = json.Unmarshal(data, &orders)
			if err != nil {
				log.Printf("Failed to unmarshal payload into orders, %s, err %v", string(data), err)
				continue
			}

			for _, order := range orders {
				w.processOrderUpdate(order)
			}
		}
	}()

	return nil
}

func (w *WsSession) processOrderUpdate(order WsOrder) {
	// Ignore, this is the initial order update message
	if order.ChangeType == "ORDER_CHANGE_TYPE_UNCHANGED" {
		return
	}

	baseCurrency, ok := findCurrency(w.currencies, order.BaseCurrency)
	if !ok {
		log.Printf("Failed to locate base currency %s in currencies map, probably needs bot restart", order.BaseCurrency)
		return
	}
	quoteCurrency, ok := findCurrency(w.currencies, order.QuoteCurrency)
	if !ok {
		log.Printf("Failed to locate quote currency %s in currencies map, probably needs bot restart", order.QuoteCurrency)
		return
	}

	symbol := pairToSymbol(baseCurrency.Tag, quoteCurrency.Tag)
	market, ok := w.symbolsMap[symbol]
	if !ok {
		log.Printf("Failed to process order update, received unknown symbol %s, probably needs restart bot", symbol)
		return
	}

	w.orderUpdateCh <- OrderUpdate{
		Market: market,
		Order:  order,
	}
}

func (w *WsSession) connectMarketDataInBatches(markets []gateway.Market, batchesOf int) error {
	// Subscribe to market data
	batches := make([]map[gateway.Market]APIPair, ((len(markets)-1)/batchesOf)+1)
	for index, market := range markets {
		group := index / batchesOf
		mkt, ok := w.symbolsMap[market.Symbol]
		if !ok {
			log.Printf("LAToken ws failed to find market %s in symbolsMap", market.Symbol)
			continue
		}
		pair, _ := w.marketsMap[mkt]

		if batches[group] == nil {
			batches[group] = make(map[gateway.Market]APIPair)
		}

		batches[group][market] = pair
	}

	log.Printf("LAToken has %d markets, will need to distribute market data subscriptions in %d websocket connections, maximum of %d subscriptions on each websocket.", len(markets), len(batches), batchesOf)

	wsDone := make(chan bool)
	wsError := make(chan error)
	wsQuit := make([]chan bool, len(batches))

	wsWg := sync.WaitGroup{}
	wsWg.Add(len(batches))

	for index, batch := range batches {
		wsQuit[index] = make(chan bool)

		go func(batch map[gateway.Market]APIPair, quit chan bool) {
			stompConn, err := w.getStompConn()
			if err != nil {
				wsError <- fmt.Errorf("Failed to open stomp conn for market data, err: %s", err)
				return
			}

			for market, pair := range batch {
				go func(market gateway.Market, pair APIPair) {
					err := w.subscribeBook(stompConn, market, pair)
					if err != nil {
						wsError <- fmt.Errorf("Failed to subscribe to %s, err: %s", market, err)
						return
					}
				}(market, pair)
			}

			wsWg.Done()

			shouldQuit := <-quit
			if shouldQuit {
				stompConn.Disconnect()
			}
		}(batch, wsQuit[index])
	}

	go func() {
		wsWg.Wait()
		wsDone <- true
	}()

	var err error
	select {
	case <-wsDone:
		log.Printf("Finished opening %d ws connections", len(batches))
	case wsErr := <-wsError:
		err = wsErr
		log.Printf("Failed failed to open ws, err: %s", wsErr)
		log.Printf("Closing all stomp ws connections...")
	}

	hasErr := err != nil
	for _, quit := range wsQuit {
		select {
		case quit <- hasErr:
		default:
			// Doesn't need to quit, never connected
		}
	}

	return err
}

func (w *WsSession) subscribeBook(stompConn *stomp.Conn, market gateway.Market, pair APIPair) error {
	dest := fmt.Sprintf("/v1/book/%s/%s", pair.BaseCurrency, pair.QuoteCurrency)

	sub, err := stompConn.Subscribe(dest, stomp.AckAuto)
	if err != nil {
		return err
	}

	go func() {
		nextNonce := int64(0)
		maxBufferedMsgs := 10
		bufferedMsgs := make(map[int64][]byte)

		for msg := range sub.C {
			if msg.Err != nil {
				err = fmt.Errorf("Failed to read market %s order book subscriber, err %s", market, msg.Err)
				log.Println(err)
				panic(err)
			}

			msgNonce, err := jsonparser.GetInt(msg.Body, "nonce")
			if err != nil {
				log.Printf("Failed to get msg \"nonce\" of book update %s, data %s, err %v", market, string(msg.Body), err)
				continue
			}

			currentNonce := nextNonce - 1

			if msgNonce == nextNonce {
				w.processDepthUpdate(market, msg.Body)
				nextNonce += 1

				// Process buffered messages
			bufMsgsLoop:
				for len(bufferedMsgs) > 0 {
					for msgNonce, msgBody := range bufferedMsgs {
						if msgNonce == nextNonce {
							log.Printf("Book %s processing buffered msg nonce %d, from %d buffered msgs", market, msgNonce, len(bufferedMsgs))

							w.processDepthUpdate(market, msgBody)
							nextNonce += 1
							delete(bufferedMsgs, msgNonce)
							continue bufMsgsLoop
						}
					}
					break bufMsgsLoop
				}
			} else if msgNonce <= currentNonce {
				err = fmt.Errorf("Book %s received nonce %d more than once, current nonce is %d!", market, msgNonce, currentNonce)
				log.Println(err)
			} else {
				// Received nonce in the future, buffer it...
				msgNonceOffset := int((msgNonce - nextNonce))

				if msgNonceOffset > maxBufferedMsgs {
					err = fmt.Errorf("Book %s received msg with nonce %d but we are still on nonce %d (offset %d), max allowed buffered msgs is %d!", market, msgNonce, currentNonce, msgNonceOffset, maxBufferedMsgs)
					panic(err)
				}

				log.Printf("Book %s buffering msg with nonce %d, current nonce %d (offset %d)", market, msgNonce, currentNonce, msgNonceOffset)

				bufferedMsgs[msgNonce] = msg.Body
			}
		}

	}()

	return nil
}

func (w *WsSession) processDepthUpdate(market gateway.Market, body []byte) {
	bids, err := w.processWsPrices(market, body, gateway.Bid)
	if err != nil {
		log.Printf("Failed to process bid data for market %s, data %s, err %v", market, string(body), err)
		return
	}

	asks, err := w.processWsPrices(market, body, gateway.Ask)
	if err != nil {
		log.Printf("Failed to process ask data for market %s, data %s, err %v", market, string(body), err)
		return
	}

	w.depthUpdateCh <- DepthUpdate{
		Market: market,
		Bids:   bids,
		Asks:   asks,
	}
}

func (w *WsSession) processWsPrices(market gateway.Market, data []byte, side gateway.Side) (prices []WsPriceLevel, err error) {
	var sideAttribute string
	if side == gateway.Ask {
		sideAttribute = "ask"
	} else {
		sideAttribute = "bid"
	}

	pxData, _, _, err := jsonparser.Get(data, "payload", sideAttribute)
	if err != nil {
		return
	}

	err = json.Unmarshal(pxData, &prices)
	if err != nil {
		return
	}

	pxsMap := make(map[float64]WsPriceLevel)
	duplicates := make([]string, 0)
	for _, px := range prices {
		currentPx, ok := pxsMap[px.Price]
		if ok {
			duplicates = append(duplicates, fmt.Sprintf("\tPrice [%s] [%f] (old amount = [%f]; new amount = [%f])", side, px.Price, currentPx.Quantity, px.Quantity))
		}

		pxsMap[px.Price] = px
	}

	if w.options.Verbose && len(duplicates) > 0 {
		log.Printf("Market [%s] received %d duplicates\n%s\nOn msg: %s", market, len(duplicates), strings.Join(duplicates, "\n"), string(data))
	}

	prices = make([]WsPriceLevel, 0, len(pxsMap))
	for _, px := range pxsMap {
		prices = append(prices, px)
	}

	return prices, nil
}

func findCurrency(currencies []APICurrency, id string) (APICurrency, bool) {
	for _, currency := range currencies {
		if currency.ID == id {
			return currency, true
		}
	}

	return APICurrency{}, false
}

func pairToSymbol(base, quote string) string {
	return fmt.Sprintf("%s_%s", base, quote)
}

// Map ws order to a order state
func mapWsOrderToState(order WsOrder) gateway.OrderState {
	return mapOrderState(order.Status, order.Quantity, order.Filled)
}
