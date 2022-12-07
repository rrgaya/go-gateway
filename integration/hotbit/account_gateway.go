package hotbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     ApiClient
	ws      *WsSession
	options gateway.Options
	tickCh  chan gateway.Tick
}

func NewAccountGateway(api ApiClient, options gateway.Options, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:     api,
		options: options,
		tickCh:  tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	session, err := g.api.SessionInfo()
	if err != nil {
		return err
	}

	onDisconnect := make(chan error)
	g.ws = NewWsSession(g.options, onDisconnect)
	err = g.ws.Connect()
	if err != nil {
		return err
	}

	go func() {
		err := <-onDisconnect
		log.Printf("Hotbit account gateway ws disconnected, err: %s", err)
		log.Printf("Hotbit reconnecting in 5 seconds...")
		close(g.ws.Message())
		time.Sleep(5 * time.Second)

		err = g.Connect()
		if err != nil {
			panic(fmt.Errorf("Hotbit failed to reconnect, err: %s", err))
		}
	}()

	go g.messageHandler()

	// Auth
	err = g.ws.Authenticate(session.UserSign)
	if err != nil {
		return err
	}

	return nil
}

func (g *AccountGateway) subscribeMarketsUpdate(markets []gateway.Market) error {
	// Subscribe
	maxPerReq := 100
	symbols := make([]string, 0, maxPerReq)
	subReqs := make([]WsRequest, 0)
	for i, market := range markets {
		symbols = append(symbols, market.Symbol)

		lastIndex := i >= len(markets)-1
		reachedMax := len(symbols) >= maxPerReq

		if reachedMax || lastIndex {
			subReqs = append(subReqs, WsRequest{
				Method: "order.subscribe",
				Params: symbols,
			})

			if lastIndex {
				break
			} else {
				symbols = make([]string, 0, maxPerReq) // Reset
			}
		}
	}

	for i, subReq := range subReqs {
		var proxy *url.URL
		if len(g.options.Proxies) > 0 {
			proxy = g.options.Proxies[i%len(g.options.Proxies)]
		}

		log.Printf("Subbing to account orders update [%d/%d] on proxy [%s]", i+1, len(subReqs), proxy)

		maxRetries := 20
		retryCount := 0
		err := g.subscribeOrders(subReq, proxy, maxRetries, retryCount)
		if err != nil {
			err = fmt.Errorf("Ws proxy [%s] subReq [%d] failed to connect, err: %s", proxy, i, err)
			return err
		}
	}

	return nil
}

func (g *AccountGateway) subscribeOrders(subReq WsRequest, proxy *url.URL, maxRetries, retryCount int) error {
	onDisconnect := make(chan error)
	ws := NewWsSession(g.options, onDisconnect)

	if proxy != nil {
		ws.SetProxy(proxy)
	}

	session, err := g.api.SessionInfo()
	if err != nil {
		err = fmt.Errorf("get session info, err: %s", err)
		return g.retrySubscribeOrders(subReq, proxy, maxRetries, retryCount, err)
	}

	err = ws.Connect()
	if err != nil {
		err = fmt.Errorf("ws connect, err: %s", err)
		return g.retrySubscribeOrders(subReq, proxy, maxRetries, retryCount, err)
	}

	go func() {
		for msg := range ws.Message() {
			if msg.Method == "order.update" {
				g.processOrderUpdate(msg.Params)
			}
		}
	}()

	err = ws.Authenticate(session.UserSign)
	if err != nil {
		err = fmt.Errorf("failed to auth ws, err: %s", err)
		return g.retrySubscribeOrders(subReq, proxy, maxRetries, retryCount, err)
	}

	res, err := ws.SendRequest(&subReq)
	if err != nil {
		err = fmt.Errorf("failed to send orders subscription request, err: %s", err)
		return g.retrySubscribeOrders(subReq, proxy, maxRetries, retryCount, err)
	}

	if !strings.Contains(string(res.Result), "success") {
		err = fmt.Errorf("Order subscription failure response, please inspect, response: %s", string(res.Result))
		return g.retrySubscribeOrders(subReq, proxy, maxRetries, retryCount, err)
	}

	retryCount = 0
	go func() {
		err := <-onDisconnect
		if g.options.Verbose {
			log.Printf("Hotbit account gateway ws [%s] disconnected, err: %s", proxy, err)
			log.Printf("Hotbit reconnecting in 5 seconds...")
		}
		close(ws.Message())
		time.Sleep(5 * time.Second)
		err = g.subscribeOrders(subReq, proxy, maxRetries, retryCount)
		if err != nil {
			err = fmt.Errorf("Hotbit failed to reconnect after disconnect, err: %s", err)
			panic(err)
		}
	}()

	return nil
}

func (g *AccountGateway) retrySubscribeOrders(subReq WsRequest, proxy *url.URL, maxRetries, retryCount int, err error) error {
	log.Printf("Hotbit ws [%s] subscribeOrders [retry count %d], err: %s", proxy, retryCount, err)
	if retryCount > maxRetries {
		err = fmt.Errorf("Reached maximum connect retry count of %d, err: %s", maxRetries, err)
		return err
	}
	time.Sleep(5 * time.Second)
	return g.subscribeOrders(subReq, proxy, maxRetries, retryCount+1)
}

func (g *AccountGateway) messageHandler() {
	for _ = range g.ws.Message() {
		// nothing to do
	}
}

func (g *AccountGateway) processOrderUpdate(data json.RawMessage) {
	var params []json.RawMessage
	err := json.Unmarshal(data, &params)
	if err != nil {
		log.Printf("Failed to unmarshal order update %s, error: %s", string(data), err)
		return
	}

	if len(params) < 2 {
		log.Printf("Failed to process order updated, expected at least an array of 2 items `%s`", string(data))
		return
	}

	var eventType int64
	err = json.Unmarshal(params[0], &eventType)
	if err != nil {
		log.Printf("Failed to unmarshal params[0] into event type %s, error: %s", string(params[0]), err)
		return
	}

	var order Order
	err = json.Unmarshal(params[1], &order)
	if err != nil {
		log.Printf("Failed to unmarshal params[1] into order %s, error: %s", string(params[1]), err)
		return
	}

	var state gateway.OrderState
	if order.Left <= 0 {
		state = gateway.OrderFullyFilled
	} else if eventType == 3 {
		state = gateway.OrderCancelled
	} else if order.DealStock > 0 {
		state = gateway.OrderPartiallyFilled
	} else {
		state = gateway.OrderOpen
	}

	avgPrice := 0.0
	if order.DealStock > 0 {
		avgPrice = order.DealMoney / order.DealStock
	}

	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(gateway.Order{
		ID:               strconv.FormatInt(order.Id, 10),
		Market:           gateway.Market{Symbol: order.Market},
		State:            state,
		Price:            order.Price,
		Amount:           order.Amount,
		AvgPrice:         avgPrice,
		FilledAmount:     order.DealStock,
		FilledMoneyValue: order.DealMoney,
	}))
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	request := NewWsRequest("asset.query", []string{})
	res, err := g.ws.SendRequest(&request)
	if err != nil {
		return balances, fmt.Errorf("failed to make assets.query to ws, err: %s", err)
	}

	var assets map[string]AssetBalance
	err = json.Unmarshal(res.Result, &assets)
	if err != nil {
		return balances, fmt.Errorf("failed to unmarshal res.Result, err: %s", err)
	}

	balances = make([]gateway.Balance, 0, len(assets))
	for asset, balance := range assets {
		balances = append(balances, gateway.Balance{
			Asset:     asset,
			Total:     balance.Available + balance.Freeze,
			Available: balance.Available,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	params := make([]interface{}, 3)
	params[0] = []string{market.Symbol}
	params[1] = 0
	params[2] = 100
	request := NewWsRequest("order.query", params)
	res, err := g.ws.SendRequest(&request)
	if err != nil {
		return orders, fmt.Errorf("failed to make order.query to ws, err: %s", err)
	}

	var results map[string]OrderQuery
	err = json.Unmarshal(res.Result, &results)
	if err != nil {
		return orders, fmt.Errorf("failed to unmarshal res.Result, err: %s", err)
	}

	result, ok := results[market.Symbol]
	if !ok {
		err = fmt.Errorf("requested asset %s was not on results %+v", market.Symbol, string(res.Result))
		return orders, err
	}

	orders = make([]gateway.Order, len(result.Records))
	for i, record := range result.Records {
		orders[i] = mapOrderToCommon(market, record)
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	retryUntil := time.Now().Add(5 * time.Second)

	for {
		if time.Now().Before(retryUntil) {
			orderId, err = g.sendOrder(order)
			if err != nil {
				switch {
				// Retry in 200ms, they are throttling us for sending orders too fast
				case strings.Contains(err.Error(), "too fast"):
					log.Printf("Failed to send order %s %g @ %g, \"too fast\" error, retrying in 200ms, err: %s", order.Market.Pair, order.Amount, order.Price, err)
					time.Sleep(200 * time.Millisecond)
					continue
				}
			}
		}

		return
	}
}

func (g *AccountGateway) sendOrder(order gateway.Order) (orderId string, err error) {
	price, err := translateOrderPrice(order)
	if err != nil {
		return
	}
	quantity, err := translateOrderAmount(order)
	if err != nil {
		return
	}
	symbol, err := translateOrderSymbol(order)
	if err != nil {
		return
	}
	side, err := translateOrderSide(order)
	if err != nil {
		return
	}
	_type, err := translateOrderType(order)
	if err != nil {
		return
	}

	id, err := g.api.CreateOrder(price, quantity, symbol, side, _type)
	if err != nil {
		return
	}

	orderId = strconv.FormatInt(id, 10)

	return
}

var detailNotFoundMatch = regexp.MustCompile(`detail not found`)

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.cancelOrderWithRetry(order, 0, 3, nil)
}

func (g *AccountGateway) cancelOrderWithRetry(order gateway.Order, retryCount, maxRetry int, lastErr error) error {
	if retryCount >= maxRetry {
		return fmt.Errorf("reached maximum retries [%d] after last err [%s]", maxRetry, lastErr)
	}

	symbol := order.Market.Symbol
	orderId, err := strconv.ParseInt(order.ID, 10, 64)
	if err != nil {
		return err
	}

	err = g.api.CancelOrder(orderId, symbol)
	if err != nil {
		switch {
		case detailNotFoundMatch.MatchString(err.Error()):
			log.Printf("Hotbit failed to cancel order [%s], retryable \"detail not found\" err, retrying in 1 second...", order)
			time.Sleep(3 * time.Second)
			return g.cancelOrderWithRetry(order, retryCount+1, maxRetry, err)
		default:
			return err
		}
	}

	return err
}

func translateOrderPrice(order gateway.Order) (string, error) {
	priceTick := order.Market.PriceTick
	tickPrecision := int64(math.Max(math.Round(math.Log10(priceTick)*-1), 0))

	priceRounded := math.Round(order.Price/priceTick) * priceTick

	return fmt.Sprintf(fmt.Sprintf("%%.%df", tickPrecision), priceRounded), nil
}

func translateOrderAmount(order gateway.Order) (string, error) {
	amountTick := order.Market.AmountTick
	tickPrecision := int64(math.Max(math.Round(math.Log10(amountTick)*-1), 0))

	amountRounded := math.Floor(order.Amount/amountTick) * amountTick // Don't round, floor, since we cannot buy/sell more than we have

	return fmt.Sprintf(fmt.Sprintf("%%.%df", tickPrecision), amountRounded), nil
}

func translateOrderSymbol(order gateway.Order) (string, error) {
	return fmt.Sprintf("%s/%s", order.Market.Pair.Base, order.Market.Pair.Quote), nil
}

func translateOrderSide(order gateway.Order) (string, error) {
	switch {
	case order.Side == gateway.Bid:
		return "BUY", nil
	case order.Side == gateway.Ask:
		return "SELL", nil
	default:
		return "", errors.New(fmt.Sprintf("Failed to translate order side: %+v", order.Side))
	}
}

// Only limit orders are supported for now
func translateOrderType(order gateway.Order) (string, error) {
	return "LIMIT", nil
}

func mapOrderToCommon(mkt gateway.Market, order Order) gateway.Order {
	state := gateway.OrderOpen
	if order.DealStock > 0 {
		state = gateway.OrderPartiallyFilled
	}

	avgPrice := 0.0
	if order.DealStock > 0 {
		avgPrice = order.DealMoney / order.DealStock
	}

	return gateway.Order{
		ID:               strconv.FormatInt(order.Id, 10),
		Market:           mkt,
		State:            state,
		Price:            order.Price,
		Amount:           order.Amount,
		AvgPrice:         avgPrice,
		FilledAmount:     order.DealStock,
		FilledMoneyValue: order.DealMoney,
	}
}
