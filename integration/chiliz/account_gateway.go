package chiliz

import (
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api        *API
	tickCh     chan gateway.Tick
	openOrders map[string]gateway.Order
	markets    []gateway.Market
}

const (
	bidSide = "BUY"
	askSide = "SELL"

	limitOrderType      = "LIMIT"
	marketOrderType     = "MARKET"
	limitMakerOrderType = "LIMIT_MAKER"
)

func NewAccountGateway(api *API, tickCh chan gateway.Tick, markets []gateway.Market) *AccountGateway {
	return &AccountGateway{
		api:    api,
		tickCh: tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	go g.trackOpenOrders()

	return nil
}

// Polling startegy applied to Chiliz API in order to be able to send
// gateway.Tick with the order updates.
func (g *AccountGateway) trackOpenOrders() {
	processOrder := func(order gateway.Order) {
		if _, ok := g.openOrders[order.ID]; ok {
			trackedOrder := g.openOrders[order.ID]

			switch {
			case trackedOrder.State != order.State || trackedOrder.FilledAmount != order.FilledAmount:
				g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
				fallthrough
			case isOpenState(order.State):
				g.openOrders[order.ID] = order
			case !isOpenState(order.State):
				delete(g.openOrders, order.ID)
			}
		}
	}

	for {
		for _, market := range g.markets {
			err := g.loadOpenOrders(market)

			if err != nil {
				panic(err)
			}

			ordersHistoryReq := OrdersHistoryRequest{
				Symbol: market.Symbol,
			}

			orders, err := g.api.FetchOrdersHistory(ordersHistoryReq)

			if err != nil {
				panic(err)
			}

			for _, order := range orders {
				gatewayOrder := chilizOrderToGatewayOrder(order, market)

				processOrder(gatewayOrder)
			}
		}

		time.Sleep(1500 * time.Millisecond)
	}
}

func (g *AccountGateway) loadOpenOrders(market gateway.Market) error {
	openOrders, err := g.OpenOrders(market)

	if err != nil {
		return err
	}

	for _, order := range openOrders {
		if _, ok := g.openOrders[order.ID]; !ok && isOpenState(order.State) {
			g.openOrders[order.ID] = order
		}
	}

	return nil
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	account, err := g.api.FetchAccount()

	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, len(account.Balances))
	for i, balance := range account.Balances {
		balances[i] = gateway.Balance{
			Asset:     balance.Asset,
			Total:     balance.Free + balance.Locked,
			Available: balance.Free,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	openOrdersreq := OpenOrdersRequest{
		Symbol: market.Symbol,
	}

	res, err := g.api.FetchOpenOrders(openOrdersreq)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))

	for i, openOrder := range res {
		orders[i] = chilizOrderToGatewayOrder(openOrder, market)
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	createOrderReq := CreateOrderRequest{
		Symbol:   order.Market.Symbol,
		Side:     gatewayOrderSideToChilizSide(order.Side),
		Type:     limitOrderType,
		Quantity: order.Amount,
		Price:    order.Price,
	}

	res, err := g.api.CreateOrder(createOrderReq)
	if err != nil {
		return "", err
	}

	return res.OrderID, err
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	orderID, err := strconv.ParseInt(order.ID, 10, 64)
	if err != nil {
		return err
	}

	cancelOrderReq := CancelOrderRequest{
		OrderID: orderID,
	}

	_, err = g.api.CancelOrder(cancelOrderReq)
	if err != nil {
		return err
	}

	return nil
}

func orderStatusToGatewayState(status string) gateway.OrderState {
	switch status {
	case "NEW":
		return gateway.OrderOpen
	case "PARTIALLY_FILLED":
		return gateway.OrderPartiallyFilled
	case "FILLED":
		return gateway.OrderFullyFilled
	case "CANCELED":
		return gateway.OrderCancelled
	case "PENDING_CANCEL":
		return gateway.OrderOpen
	case "REJECTED":
		return gateway.OrderCancelled
	}

	return ""
}

func gatewayOrderSideToChilizSide(side gateway.Side) string {
	if side == gateway.Ask {
		return askSide
	} else {
		return bidSide
	}
}

func chilizSideToGatewaySide(side string) gateway.Side {
	if side == bidSide {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func isOpenState(state gateway.OrderState) bool {
	if state == gateway.OrderOpen || state != gateway.OrderPartiallyFilled {
		return true
	}

	return false
}

func chilizOrderToGatewayOrder(order Order, market gateway.Market) gateway.Order {
	return gateway.Order{
		Market:       market,
		ID:           order.OrderID,
		State:        orderStatusToGatewayState(order.Status),
		Amount:       order.OrigQty,
		Price:        order.Price,
		FilledAmount: order.ExecutedQty,
		Side:         chilizSideToGatewaySide(order.Side),
	}
}
