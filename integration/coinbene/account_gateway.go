package coinbene

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	options         gateway.Options
	api             *API
	ws              *WsSession
	openOrders      map[string]gateway.Order
	openOrdersMutex *sync.RWMutex
	tickCh          chan gateway.Tick
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:         options,
		api:             api,
		tickCh:          tickCh,
		openOrders:      make(map[string]gateway.Order),
		openOrdersMutex: &sync.RWMutex{},
	}
}

func (g *AccountGateway) Connect() error {
	g.ws = NewWsSession()
	err := g.ws.Connect()
	if err != nil {
		return err
	}

	err = g.ws.Authenticate(g.options.ApiKey, g.options.ApiSecret)
	if err != nil {
		return err
	}

	// Set initial open orders state
	err = g.resetOpenOrders()
	if err != nil {
		log.Printf("Coinbene failed to fetch open orders, err: %s", err)
		return err
	}

	// Subscribe to user order updates
	err = g.subscribeOrderUpdates()
	if err != nil {
		return err
	}

	return nil
}

func (g *AccountGateway) subscribeOrderUpdates() error {
	userOrderUpdateCh := make(chan bool, 100)

	err := g.ws.SubscribeUserOrderChange(userOrderUpdateCh, nil)
	if err != nil {
		return err
	}

	go func() {
		for _ = range userOrderUpdateCh {
			// Check if we have any open orders, if not
			// we should not check for open orders updates.
			// Since we have received a order update signal
			// from an order we did not open.
			if g.noOpenOrders() {
				log.Printf("Coinbene received orders update signal, without having any open orders, ignoring...")
				continue
			}

			err := g.updateOpenOrders()
			if err != nil {
				log.Printf("Coinbene FAILED to update open orders, err: %s", err)
			}

			// Reset excessive user order update requests
			for len(userOrderUpdateCh) > 2 {
				<-userOrderUpdateCh
			}
		}
	}()

	return nil
}

func (g *AccountGateway) noOpenOrders() bool {
	g.openOrdersMutex.RLock()
	defer g.openOrdersMutex.RUnlock()

	return len(g.openOrders) == 0
}

func (g *AccountGateway) resetOpenOrders() error {
	g.openOrdersMutex.Lock()
	defer g.openOrdersMutex.Unlock()

	g.openOrders = make(map[string]gateway.Order)
	orders, err := g.api.OpenOrders("") // Fetch all open orders, from all markets
	if err != nil {
		return err
	}

	for _, order := range orders {
		commonOrder := mapOrderToCommon(order)
		if commonOrder.State == gateway.OrderOpen || commonOrder.State == gateway.OrderPartiallyFilled {
			g.openOrders[order.OrderID] = commonOrder
		}
	}

	return nil
}

func fetchApiOrdersWithRetry(fn func(symbol string) ([]Order, error), timeout time.Duration, maxRetries int) (orders []Order, err error) {
	retryCount := 0

	for retryCount < maxRetries {
		orders, err = fn("")
		if err == nil {
			return
		}

		log.Printf(
			"Error: \"%s\" on fetchApiOrdersWithRetry(%s), retries %d/%d, retrying in %v...",
			err,
			runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(),
			retryCount,
			maxRetries,
			timeout,
		)

		retryCount += 1
		time.Sleep(timeout)
	}

	err = fmt.Errorf("err %s after %d max retries", err, maxRetries)

	return
}

func (g *AccountGateway) updateOpenOrders() error {
	openOrders, err := fetchApiOrdersWithRetry(g.api.OpenOrders, time.Duration(150*time.Millisecond), 10)
	if err != nil {
		return fmt.Errorf("failed to fetch open orders, err: %s", err)
	}

	closedOrders, err := fetchApiOrdersWithRetry(g.api.ClosedOrders, time.Duration(150*time.Millisecond), 10)
	if err != nil {
		return fmt.Errorf("failed to fetch open orders, err: %s", err)
	}

	// Diff received open orders with currently tracked open orders
	// Remove any closed orders from our open orders
	// Look for changes and dispatch order updates
	g.openOrdersMutex.Lock()
	for _, openOrder := range openOrders {
		for _, trackedOrder := range g.openOrders {
			if openOrder.OrderID == trackedOrder.ID {
				order := mapOrderToCommon(openOrder)

				// Check if currently traacked order was filled
				if trackedOrder.FilledAmount != order.FilledAmount {
					go g.dispatchOrderUpdate(order)
					g.openOrders[openOrder.OrderID] = order
				}
			}
		}
	}
	for _, closedOrder := range closedOrders {
		for _, trackedOrder := range g.openOrders {
			if closedOrder.OrderID == trackedOrder.ID {
				order := mapOrderToCommon(closedOrder)
				go g.dispatchOrderUpdate(order)
				delete(g.openOrders, closedOrder.OrderID)
			}
		}
	}
	g.openOrdersMutex.Unlock()

	return nil
}

func (g *AccountGateway) dispatchOrderUpdate(order gateway.Order) {
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.api.AccountBalance()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances = make([]gateway.Balance, 0, len(res))
	for _, balance := range res {
		balances = append(balances, gateway.Balance{
			Asset:     balance.Asset,
			Total:     balance.Available + balance.FrozenBalance,
			Available: balance.TotalBalance,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	res, err := g.api.OpenOrders(market.Symbol)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))
	for index, resOrder := range res {
		orders[index] = mapOrderToCommon(resOrder)
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (string, error) {
	req := PlaceOrder{
		Symbol:    order.Market.Symbol,
		Direction: mapCommonSideToIntDirection(order.Side),
		Price:     utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
		Quantity:  utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		OrderType: "1", // Limit order
	}

	orderID, err := g.api.PlaceOrder(req)
	if err != nil {
		return orderID, err
	}

	// Register open order
	g.openOrdersMutex.Lock()
	order.ID = orderID
	g.openOrders[orderID] = order
	g.openOrdersMutex.Unlock()

	return orderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder(order.ID)
}

func mapOrderToCommon(order Order) gateway.Order {
	avgPx, _ := order.AvgPrice.Float64()

	return gateway.Order{
		ID:               order.OrderID,
		State:            mapOrderStatusToCommon(order.OrderStatus),
		Side:             mapOrderSideToCommon(order.OrderDirection),
		Amount:           order.Quantity,
		Price:            order.OrderPrice,
		AvgPrice:         avgPx,
		FilledAmount:     order.FilledQuantity,
		FilledMoneyValue: order.FilledAmount,
	}
}

func mapOrderStatusToCommon(status string) gateway.OrderState {
	switch status {
	case "Open":
		return gateway.OrderOpen
	case "PartiallyFilled":
		return gateway.OrderPartiallyFilled
	case "Filled":
		return gateway.OrderFullyFilled
	case "Partially cancelled":
		return gateway.OrderCancelled
	case "Cancelled":
		return gateway.OrderCancelled
	default:
		log.Printf("Coinbene unable to map order status %s to gateway order state", status)
	}

	return ""
}

func mapOrderSideToCommon(side string) gateway.Side {
	if side == "buy" {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func mapIntOrderSideToCommon(side int64) gateway.Side {
	if side == 1 {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func mapCommonSideToDirection(side gateway.Side) string {
	if side == gateway.Bid {
		return "buy"
	} else if side == gateway.Ask {
		return "sell"
	}

	return ""
}

func mapCommonSideToIntDirection(side gateway.Side) string {
	if side == gateway.Bid {
		return "1"
	} else if side == gateway.Ask {
		return "2"
	}

	return ""
}
