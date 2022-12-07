package bitcointrade

import (
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

var insuficientBalanceMatch = regexp.MustCompile(`Saldo insuficiente`)

type AccountGateway struct {
	gateway.BaseAccountGateway
	options         gateway.Options
	markets         []gateway.Market
	api             *API
	trackedMarkets  map[string]gateway.Market
	openOrders      map[string]gateway.Order
	openOrdersMutex *sync.Mutex
	userID          string
	tickCh          chan gateway.Tick
}

func NewAccountGateway(options gateway.Options, markets []gateway.Market, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:         options,
		markets:         markets,
		api:             api,
		trackedMarkets:  make(map[string]gateway.Market),
		openOrders:      make(map[string]gateway.Order),
		openOrdersMutex: &sync.Mutex{},
		tickCh:          tickCh,
	}
}

func (g *AccountGateway) SetUserID(val string) {
	g.userID = val
}

func (g *AccountGateway) Connect() error {
	return nil
}

func (g *AccountGateway) ProcessOrderUpdate(topic string, update WSOrder) {
	if update.UserCode != g.userID {
		return
	}

	g.openOrdersMutex.Lock()
	trackedOrder, ok := g.openOrders[update.Code]
	if ok {
		if topic == "cancel_order" {
			trackedOrder.State = gateway.OrderCancelled
			g.openOrders[trackedOrder.ID] = trackedOrder
			g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(trackedOrder))
		}
	}
	defer g.openOrdersMutex.Unlock()
}

func (g *AccountGateway) ProcessOrderCompleted(topic string, update WSOrderCompleted) {
	if !(update.ActiveOrderUserCode == g.userID || update.PassiveOrderUserCode == g.userID) {
		return
	}

	go func() {
		// Dirty hack, we will only do this, because speed doesnt matter here
		// we receive order_completed updates, before receiving the order open
		// api response, with it's ID to match.
		time.Sleep(100 * time.Millisecond)

		g.openOrdersMutex.Lock()
		trackedOrder, ok := g.openOrders[update.ActiveOrderCode]
		if !ok {
			trackedOrder, ok = g.openOrders[update.PassiveOrderCode]
		}
		if ok {
			trackedOrder.FilledAmount += update.Amount
			if trackedOrder.FilledAmount >= trackedOrder.Amount {
				trackedOrder.State = gateway.OrderFullyFilled
			} else {
				trackedOrder.State = gateway.OrderPartiallyFilled
			}
			g.openOrders[trackedOrder.ID] = trackedOrder
			g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(trackedOrder))
		}
		defer g.openOrdersMutex.Unlock()

	}()
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.WalletsBalance()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0)
	for _, balance := range res {
		balances = append(balances, gateway.Balance{
			Asset:     balance.CurrencyCode,
			Total:     balance.AvailableAmount + balance.LockedAmount,
			Available: balance.AvailableAmount,
		})
	}

	return balances, nil
}

func (g *AccountGateway) getOrdersByStatus(market gateway.Market, status string) ([]gateway.Order, error) {
	params := url.Values{}
	params.Set("pair", market.Symbol)
	params.Set("status", status)
	params.Set("page_size", "200")
	res, err := g.api.ListUserOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, 0)
	for _, resOrder := range res {
		orders = append(orders, mapOrderToCommon(market, resOrder))
	}

	return orders, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	openOrders, err := g.getOrdersByStatus(market, "waiting")
	if err != nil {
		return []gateway.Order{}, err
	}

	partiallyFilledOrders, err := g.getOrdersByStatus(market, "executed_partially")
	if err != nil {
		return []gateway.Order{}, err
	}

	return append(openOrders, partiallyFilledOrders...), nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	var side string
	if order.Side == gateway.Bid {
		side = "buy"
	} else {
		side = "sell"
	}

	params := APICreateOrder{
		Pair:      order.Market.Symbol,
		Subtype:   "limited",
		Type:      side,
		Amount:    order.Amount,
		UnitPrice: order.Price,
	}

	resOrder, err := g.api.CreateOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	// Register open order
	commonOrder := gateway.Order{
		ID:     resOrder.Code,
		Market: order.Market,
		Side:   order.Side,
		Amount: resOrder.Amount,
		Price:  order.Price,
		State:  gateway.OrderOpen,
	}
	g.openOrdersMutex.Lock()
	g.openOrders[resOrder.Code] = commonOrder
	g.openOrdersMutex.Unlock()

	return resOrder.Code, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	params := APICancelOrder{Code: order.ID}
	_, err := g.api.CancelOrder(params)

	return err
}

func mapOrderToCommon(market gateway.Market, order APIOrder) gateway.Order {
	return gateway.Order{
		Market:       market,
		ID:           order.Code,
		Side:         mapTypeToCommonSide(order.Type),
		State:        mapOrderToCommonState(order),
		Amount:       order.RequestedAmount,
		Price:        order.UnitPrice,
		FilledAmount: order.ExecutedAmount,
	}
}

func mapOrderToCommonState(order APIOrder) gateway.OrderState {
	switch order.Status {
	case "executed_completely":
		return gateway.OrderFullyFilled
	case "executed_partially":
		return gateway.OrderPartiallyFilled
	case "waiting":
		return gateway.OrderOpen
	case "canceled":
		return gateway.OrderCancelled
	}
	return ""
}

func mapTypeToCommonSide(t string) gateway.Side {
	if t == "bid" {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}
