package novadax

import (
	"fmt"
	"regexp"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

var insuficientBalanceMatch = regexp.MustCompile(`Insufficient balance`)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api        *API
	tickCh     chan gateway.Tick
	openOrders map[string]gateway.Order
	markets    map[string]gateway.Market
}

func NewAccountGateway(api *API, tickCh chan gateway.Tick, markets map[string]gateway.Market) *AccountGateway {
	return &AccountGateway{
		api:        api,
		tickCh:     tickCh,
		openOrders: make(map[string]gateway.Order),
	}
}

func (g *AccountGateway) Connect() error {
	go g.trackOpenOrders()

	return nil
}

func (g *AccountGateway) loadOpenOrders() {
	paramsReq := ApiGetOrderHistoryRequest{
		Status: "UNFINISHED",
	}

	orders, err := g.api.GetOrderHistory(paramsReq)
	if err != nil {
		panic(err)
	}

	for _, order := range orders {
		market := g.markets[order.Symbol]

		gtwOrder := g.parseToGatewayOrder(order, market)
		g.openOrders[gtwOrder.ID] = gtwOrder
	}
}

func (g *AccountGateway) trackOpenOrders() {
	g.loadOpenOrders()

	for {
		paramsReq := ApiGetOrderHistoryRequest{}

		orders, err := g.api.GetOrderHistory(paramsReq)
		if err != nil {
			fmt.Println(err)
			continue
		}

		for _, order := range orders {
			market := g.markets[order.Symbol]

			trackedOrder, ok := g.openOrders[order.Symbol]

			if ok {
				updatedOrder := g.parseToGatewayOrder(order, market)
				hasOrderChanged := trackedOrder.State != updatedOrder.State ||
					trackedOrder.FilledAmount != updatedOrder.FilledAmount

				if hasOrderChanged {
					if g.isOrderOpen(updatedOrder) {
						g.openOrders[order.Symbol] = updatedOrder
					} else {
						delete(g.openOrders, updatedOrder.ID)
					}

					g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(updatedOrder))
				}
			}
		}

		time.Sleep(1500 * time.Millisecond)
	}
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	balances, err := g.api.GetBalances()
	if err != nil {
		return nil, err
	}

	gtwBalances := make([]gateway.Balance, len(balances))

	for i, b := range balances {
		gtwBalances[i] = gateway.Balance{
			Asset:     b.Currency,
			Available: b.Available,
			Total:     b.Available + b.Hold,
		}
	}

	return gtwBalances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	paramsReq := ApiGetOrderHistoryRequest{
		Status: "UNFINISHED",
	}

	orders, err := g.api.GetOrderHistory(paramsReq)
	if err != nil {
		return nil, err
	}

	gtwOrders := make([]gateway.Order, len(orders))

	for i, order := range orders {
		gtwOrders[i] = g.parseToGatewayOrder(order, market)
	}

	return gtwOrders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (string, error) {
	paramsReq := ApiOrderRequest{
		Symbol: order.Market.Symbol,
		Type:   "LIMIT",
		Side:   g.toExchangeOrderSide(order.Side),
		Amount: utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		Price:  utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
	}

	apiOrder, err := g.api.PostOrder(paramsReq)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return apiOrder.Id, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	err := g.api.CancelOrder(order.ID)

	return err
}

func (g *AccountGateway) parseToGatewayOrder(order ApiOrder, market gateway.Market) gateway.Order {
	return gateway.Order{
		Market:       market,
		ID:           order.Id,
		State:        g.toGatewayState(order.Status),
		Amount:       order.Amount,
		Price:        order.Price,
		FilledAmount: order.FilledAmount,
		Side:         g.toGatewaySide(order.Side),
	}
}

func (g *AccountGateway) toGatewayState(status string) gateway.OrderState {
	switch status {
	case "SUBMITTED", "PROCESSING":
		return gateway.OrderOpen
	case "PARTIAL_FILLED":
		return gateway.OrderPartiallyFilled
	case "PARTIAL_CANCELED", "PARTIAL_REJECTED", "CANCELED", "CANCELING", "REJECTED":
		return gateway.OrderCancelled
	case "FILLED":
		return gateway.OrderFullyFilled
	default:
		return gateway.OrderUnknown
	}
}

func (g *AccountGateway) isOrderOpen(order gateway.Order) bool {
	return order.State == gateway.OrderCancelled || order.State == gateway.OrderFullyFilled
}

func (g *AccountGateway) toGatewaySide(side string) gateway.Side {
	if side == "BUY" {
		return gateway.Bid
	}

	return gateway.Ask
}

func (g *AccountGateway) toExchangeOrderSide(side gateway.Side) string {
	if side == gateway.Bid {
		return "BUY"
	}

	return "SELL"
}
