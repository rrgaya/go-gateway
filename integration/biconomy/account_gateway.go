package biconomy

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api           *API
	tickCh        chan gateway.Tick
	options       gateway.Options
	trackedOrders sync.Map
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:       options,
		api:           api,
		tickCh:        tickCh,
		trackedOrders: sync.Map{},
	}
}

func (g *AccountGateway) Connect() error {
	go g.trackOrderUpdates()

	return nil
}

func (g *AccountGateway) trackOrderUpdates() {
	var refreshInterval time.Duration
	if g.options.RefreshIntervalMs > 0 {
		refreshInterval = time.Duration(g.options.RefreshIntervalMs * int(time.Millisecond))
	} else {
		refreshInterval = 2500 * time.Millisecond
	}

	log.Printf("Biconomy polling for account executions every %v", refreshInterval)

	for {
		markets := make(map[string]gateway.Market)
		g.trackedOrders.Range(func(orderID any, val any) bool {
			order := val.(gateway.Order)

			markets[order.Market.Symbol] = order.Market
			return true
		})

		for _, market := range markets {
			g.updateTrackedMarket(market)
		}

		time.Sleep(refreshInterval)
	}
}

func (g *AccountGateway) updateTrackedMarket(market gateway.Market) {
	params := make(map[string]string)
	params["market"] = market.Symbol
	params["limit"] = "100"

	res, err := g.api.OpenOrders(params)
	if err != nil {
		log.Printf("%s failed to fetch open orders [%s] while order updating, err %s", Exchange, market.Symbol, err)
		return
	}

	openOrders := mapOpenAPIOrdersToGtw(market, res)

	params = make(map[string]string)
	params["market"] = market.Symbol
	params["limit"] = "100"

	res, err = g.api.FinishedOrders(params)
	if err != nil {
		log.Printf("%s failed to fetch finished orders [%s] while order updating, err %s", Exchange, market.Symbol, err)
		return
	}

	finishedOrders := mapFinisedAPIOrdersToGtw(market, res)

	events := make([]gateway.Event, 0)

	for _, order := range openOrders {
		_, ok := g.trackedOrders.Load(order.ID)

		if ok {
			events = append(events, gateway.NewOrderUpdateEvent(order))
		}
	}

	// Final track, order wont update anymore
	for _, order := range finishedOrders {
		_, ok := g.trackedOrders.LoadAndDelete(order.ID)

		if ok {
			events = append(events, gateway.NewOrderUpdateEvent(order))
		}
	}

	if len(events) > 0 {
		g.tickCh <- gateway.TickWithEvents(events...)
	}
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.Balance()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0, len(res))
	for _, balance := range res {
		balances = append(balances, gateway.Balance{
			Asset:     strings.ToUpper(balance.Asset),
			Available: balance.Available,
			Total:     balance.Available + balance.Freeze,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := make(map[string]string)
	params["market"] = market.Symbol
	params["limit"] = "100"

	res, err := g.api.OpenOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := mapOpenAPIOrdersToGtw(market, res)

	return orders, nil
}

func mapOpenAPIOrdersToGtw(market gateway.Market, res []APIOrder) []gateway.Order {
	orders := make([]gateway.Order, len(res))
	for index, resOrder := range res {
		orderState := gateway.OrderOpen
		if resOrder.DealStock > 0 {
			orderState = gateway.OrderPartiallyFilled
		}

		orders[index] = mapAPIOrderToGtw(orderState, market, resOrder)
	}

	return orders
}

func mapFinisedAPIOrdersToGtw(market gateway.Market, res []APIOrder) []gateway.Order {
	orders := make([]gateway.Order, len(res))
	for index, resOrder := range res {
		orderState := gateway.OrderCancelled
		if resOrder.DealStock >= resOrder.Amount {
			orderState = gateway.OrderFullyFilled
		}

		orders[index] = mapAPIOrderToGtw(orderState, market, resOrder)
	}

	return orders
}

func mapAPIOrderToGtw(orderState gateway.OrderState, market gateway.Market, resOrder APIOrder) gateway.Order {
	return gateway.Order{
		Market:           market,
		ID:               strconv.FormatInt(resOrder.ID, 10),
		State:            orderState,
		Amount:           resOrder.Amount,
		Price:            resOrder.Price,
		FilledAmount:     resOrder.DealStock,
		FilledMoneyValue: resOrder.DealMoney,
	}
}

var insuficientBalanceMatch = regexp.MustCompile(`balance`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)

	params["market"] = order.Market.Symbol

	if order.Side == gateway.Bid {
		params["side"] = "2" // BID
	} else {
		params["side"] = "1" // ASK
	}

	params["amount"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
	params["price"] = utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)

	res, err := g.api.CreateOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	orderID := strconv.FormatInt(res.ID, 10)

	order.ID = orderID
	g.trackedOrders.LoadOrStore(order.ID, order)

	return orderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	params := make(map[string]string)

	params["market"] = order.Market.Symbol
	params["order_id"] = order.ID

	res, err := g.api.CancelOrder(params)
	if err != nil {
		return err
	}

	orderID := strconv.FormatInt(res.ID, 10)

	if orderID != order.ID {
		return fmt.Errorf("expected to have cancelled order id [%s] instead cancelled order id [%s]", order.ID, orderID)
	}

	return nil
}
