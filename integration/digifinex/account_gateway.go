package digifinex

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     APIClient
	ws      *WsSession
	options gateway.Options
	tickCh  chan gateway.Tick
}

func NewAccountGateway(api APIClient, options gateway.Options, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:     api,
		options: options,
		tickCh:  tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	return nil
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.api.SpotAssets()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, 0)
	for _, balance := range res {
		balances = append(balances, gateway.Balance{
			Asset:     balance.Currency,
			Total:     balance.Total,
			Available: balance.Free,
		})
	}

	return balances, nil
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
	ws := NewWsSession(g.options)

	if proxy != nil {
		ws.SetProxy(proxy)
	}

	err := ws.Connect()
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

	err = ws.Authenticate(g.options.ApiKey, g.options.ApiSecret)
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

	onDisconnect := make(chan error)
	ws.SetOnDisconnect(onDisconnect)

	retryCount = 0
	go func() {
		err := <-onDisconnect
		if g.options.Verbose {
			log.Printf("DigiFinex account gateway ws [%s] disconnected, err: %s", proxy, err)
			log.Printf("DigiFinex reconnecting in 5 seconds...")
		}
		close(ws.Message())
		time.Sleep(3 * time.Second)
		err = g.subscribeOrders(subReq, proxy, maxRetries, retryCount)
		if err != nil {
			err = fmt.Errorf("DigiFinex failed to reconnect after disconnect, err: %s", err)
			panic(err)
		}
	}()

	return nil
}

func (g *AccountGateway) retrySubscribeOrders(subReq WsRequest, proxy *url.URL, maxRetries, retryCount int, err error) error {
	log.Printf("DigiFinex ws [%s] subscribeOrders [retry count %d], err: %s", proxy, retryCount, err)
	if retryCount > maxRetries {
		err = fmt.Errorf("Reached maximum connect retry count of %d, err: %s", maxRetries, err)
		return err
	}
	time.Sleep(3 * time.Second)
	return g.subscribeOrders(subReq, proxy, maxRetries, retryCount+1)
}

func (g *AccountGateway) processOrderUpdate(data json.RawMessage) {
	var orders []WsOrder
	err := json.Unmarshal(data, &orders)
	if err != nil {
		log.Printf("Failed to unmarshal order update %s, error: %s", string(data), err)
		return
	}

	for _, order := range orders {
		g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(gateway.Order{
			ID:               order.ID,
			Market:           gateway.Market{Symbol: order.Symbol},
			Side:             mapWsOrderSide(order.Side),
			State:            mapWsOrderStatusToState(order.Status),
			Price:            order.Price,
			Amount:           order.Amount,
			AvgPrice:         order.PriceAvg,
			FilledAmount:     order.Filled,
			FilledMoneyValue: order.Filled * order.PriceAvg,
		}))
	}
}

func mapWsOrderSide(side string) gateway.Side {
	if side == "buy" {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func mapWsOrderStatusToState(status int64) gateway.OrderState {
	switch status {
	case 0:
		return gateway.OrderOpen
	case 1:
		return gateway.OrderPartiallyFilled
	case 2:
		return gateway.OrderFullyFilled
	case 3:
		return gateway.OrderCancelled
	case 4:
		return gateway.OrderCancelled
	}
	return gateway.OrderUnknown
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	apiOrders, err := g.api.CurrentOrders("spot", market.Symbol)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders = make([]gateway.Order, len(apiOrders))
	for i, record := range apiOrders {
		orders[i] = mapAPIOrderToGtw(record)
	}

	return orders, nil
}

func mapOrderToAPIOrderType(order gateway.Order) string {
	if order.Side == gateway.Bid {
		return "buy"
	} else {
		return "sell"
	}
}

var insuficientBalanceMatch = regexp.MustCompile(`20011`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := &url.Values{}
	params.Set("symbol", order.Market.Symbol)
	params.Set("type", mapOrderToAPIOrderType(order))
	params.Set("amount", utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick))
	params.Set("price", utils.FloatToStringWithTick(order.Price, order.Market.PriceTick))

	if order.PostOnly {
		params.Set("post_only", "1")
	}

	res, err := g.api.NewOrder("spot", params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return res, err
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder("spot", order.ID)
}

func mapAPIOrderToGtw(order APIOrder) gateway.Order {
	return gateway.Order{
		ID:               order.OrderID,
		Market:           gateway.Market{Symbol: order.Symbol},
		Side:             mapWsOrderSide(order.Type),
		State:            mapWsOrderStatusToState(order.Status),
		Price:            order.Price,
		Amount:           order.Amount,
		AvgPrice:         order.AvgPrice,
		FilledAmount:     order.ExecutedAmount,
		FilledMoneyValue: order.ExecutedAmount * order.AvgPrice,
	}
}
