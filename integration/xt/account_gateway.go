package xt

import (
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api                *API
	options            gateway.Options
	tickCh             chan gateway.Tick
	trackedOrders      map[string]gateway.Order
	trackedOrdersMutex *sync.Mutex
}

func NewAccountGateway(api *API, options gateway.Options, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:                api,
		options:            options,
		tickCh:             tickCh,
		trackedOrders:      make(map[string]gateway.Order),
		trackedOrdersMutex: &sync.Mutex{},
	}
}

func (g *AccountGateway) Connect() error {
	go g.trackOpenOrders()

	return nil
}

func (g *AccountGateway) trackOpenOrders() {
	var refreshInterval time.Duration
	if g.options.RefreshIntervalMs > 0 {
		refreshInterval = time.Duration(g.options.RefreshIntervalMs * int(time.Millisecond))
	} else {
		refreshInterval = 1500 * time.Millisecond
	}

	// Subscribe to market updates
	log.Printf("XT polling for order updates every %v", refreshInterval)

	for {
		g.trackedOrdersMutex.Lock()
		orderIDs := make([]string, 0, len(g.trackedOrders))
		for orderID, _ := range g.trackedOrders {
			orderIDs = append(orderIDs, orderID)
		}
		g.trackedOrdersMutex.Unlock()

		if len(orderIDs) > 0 {
			g.updateOrdersBatch(orderIDs)
		}
		time.Sleep(refreshInterval)
	}
}

func (g *AccountGateway) updateOrdersBatch(orderIDs []string) {
	maxPerGroup := 100

	if len(orderIDs) > maxPerGroup {
		log.Printf("Warning XT has [%d] tracked orders, more than [%d] limit!", len(orderIDs), maxPerGroup)
	}

	updateOrdersGroup := make(map[int][]string)
	for i, id := range orderIDs {
		groupIndex := i % maxPerGroup
		if _, ok := updateOrdersGroup[groupIndex]; !ok {
			updateOrdersGroup[groupIndex] = make([]string, 0)
		}

		updateOrdersGroup[groupIndex] = append(updateOrdersGroup[groupIndex], id)
	}

	for _, group := range updateOrdersGroup {
		err := g.updateOrders(group)
		if err != nil {
			log.Printf("XT failed to update orders [%s], err: %s", group, err)
		}
	}
}

func (g *AccountGateway) updateOrders(ids []string) error {
	res, err := g.api.GetOrdersByIDS(ids)
	if err != nil {
		return err
	}

	events := make([]gateway.Event, 0)
	for _, update := range res {
		g.trackedOrdersMutex.Lock()
		currentOrder := mapAPIOrderToGtw(update)
		trackedOrder := g.trackedOrders[currentOrder.ID]

		if trackedOrder.State != currentOrder.State || trackedOrder.FilledAmount != currentOrder.FilledAmount {
			events = append(events, gateway.NewOrderUpdateEvent(currentOrder))
		}

		if currentOrder.State == gateway.OrderOpen || currentOrder.State == gateway.OrderPartiallyFilled {
			// Update order state and keep tracking it
			g.trackedOrders[currentOrder.ID] = currentOrder
		} else {
			// Final state, stop tracking
			delete(g.trackedOrders, currentOrder.ID)
		}
		g.trackedOrdersMutex.Unlock()
	}

	if len(events) > 0 {
		g.tickCh <- gateway.TickWithEvents(events...)
	}

	return nil
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.api.Balances()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, 0)
	for _, asset := range res.Assets {
		balances = append(balances, gateway.Balance{
			Asset:     strings.ToUpper(asset.Currency),
			Total:     asset.TotalAmount,
			Available: asset.AvailableAmount,
		})
	}

	return balances, nil
}

func mapOrderToState(state string) gateway.OrderState {
	switch state {
	case "NEW":
		return gateway.OrderOpen
	case "PARTIALLY_FILLED":
		return gateway.OrderPartiallyFilled
	case "FILLED":
		return gateway.OrderFullyFilled
	case "CANCELED":
		return gateway.OrderCancelled
	case "REJECTED":
		return gateway.OrderCancelled
	case "EXPIRED":
		return gateway.OrderCancelled
	}
	return gateway.OrderUnknown
}

func mapOrderTypeToSide(_side string) gateway.Side {
	if _side == "BUY" {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func mapAPIOrderToGtw(order APIOrder) gateway.Order {
	return gateway.Order{
		ID:               order.OrderID,
		Side:             mapOrderTypeToSide(order.Side),
		State:            mapOrderToState(order.State),
		Price:            order.Price,
		Amount:           order.OrigQty,
		AvgPrice:         order.AvgPrice,
		FilledAmount:     order.ExecutedQty,
		FilledMoneyValue: order.ExecutedQty * order.AvgPrice,
		Fee:              order.Fee,
		FeeAsset:         order.FeeCurrency,
	}
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	params := make(map[string]string)
	params["market"] = market.Symbol

	apiOrders, err := g.api.OpenOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders = make([]gateway.Order, len(apiOrders))
	for i, record := range apiOrders {
		order := mapAPIOrderToGtw(record)
		order.Market = market
		orders[i] = order
	}

	return orders, nil
}

func mapOrderSideToAPI(side gateway.Side) string {
	if side == gateway.Bid {
		return "BUY"
	} else {
		return "SELL"
	}
}

func mapOrderTypeToAPI(_type gateway.OrderType) string {
	if _type == gateway.MarketOrder {
		return "MARKET"
	} else {
		return "LIMIT"
	}
}

var insuficientBalanceMatch = regexp.MustCompile(`balance`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)
	params["symbol"] = order.Market.Symbol
	params["side"] = mapOrderSideToAPI(order.Side)
	params["type"] = mapOrderTypeToAPI(order.Type)
	params["timeInForce"] = "GTC"
	params["bizType"] = "SPOT"
	params["quantity"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
	params["price"] = utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)

	res, err := g.api.NewOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	// Track order state updates
	g.trackedOrdersMutex.Lock()
	g.trackedOrders[res] = gateway.Order{
		ID:     res,
		Market: order.Market,
		Amount: order.Amount,
		Price:  order.Price,
	}
	g.trackedOrdersMutex.Unlock()

	return res, err
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder(order.ID)
}
