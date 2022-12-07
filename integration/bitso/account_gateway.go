package bitso

import (
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	limitOrder  = "limit"
	marketOrder = "market"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api        *API
	tickCh     chan gateway.Tick
	openOrders map[string]gateway.Order
	markets    []gateway.Market
}

func NewAccountGateway(api *API, tickCh chan gateway.Tick, markets []gateway.Market) *AccountGateway {
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

func (g *AccountGateway) trackOpenOrders() {
	processOrder := func(trackedOrder, order gateway.Order) {
		switch {
		case isOrderUpdate(trackedOrder, order):
			g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
			fallthrough
		case isOpenState(order.State):
			g.openOrders[order.ID] = order
		case isClosedState(order.State):
			delete(g.openOrders, order.ID)
		}
	}

	for {
		oids := make([]string, 0, len(g.openOrders))
		for key := range g.openOrders {
			oids = append(oids, key)
		}

		if len(oids) > 0 {
			resOrders, err := g.api.LookupOrders(oids)
			if err != nil {
				log.Printf("Bitso failed to lookup orders [%s], err: %s", oids, err)
				time.Sleep(3000 * time.Millisecond)
				continue
			}

			for _, trackedOrder := range g.openOrders {
				var resOrder ApiOrder
				var ok bool
				for _, o := range resOrders {
					if o.ID == trackedOrder.ID {
						resOrder = o
						ok = true
						break
					}
				}

				if ok {
					newOrder := bitsoOrderToGatewayOrder(resOrder, trackedOrder.Market)
					processOrder(trackedOrder, newOrder)
				} else {
					log.Printf("Bitso trackedOrder [%s] was not present on resOrders list, must be cancelled, removing from open orders...", trackedOrder.ID)
					delete(g.openOrders, trackedOrder.ID)
				}
			}

			time.Sleep(1500 * time.Millisecond)
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	res, err := g.api.FetchOpenOrders(market.Symbol)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))

	for i, openOrder := range res {
		orders[i] = bitsoOrderToGatewayOrder(openOrder, market)
	}

	return orders, nil
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	balances, err := g.api.FetchBalances()

	if err != nil {
		return []gateway.Balance{}, err
	}

	gatewayBalances := make([]gateway.Balance, len(balances))
	for i, balance := range balances {
		gatewayBalances[i] = gateway.Balance{
			Asset:     strings.ToUpper(balance.Currency),
			Total:     balance.Total,
			Available: balance.Available,
		}
	}

	return gatewayBalances, nil
}

var insuficientBalanceMatch = regexp.MustCompile(`exceeds available`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	createOrderReq := ApiCreateOrderRequest{
		Symbol: order.Market.Symbol,
		Side:   gatewaySideToBitsoSide(order.Side),
		Type:   "limit",
		Major:  utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		Price:  utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
	}

	if order.PostOnly {
		createOrderReq.TimeInForce = "postonly"
	}

	oid, err := g.api.CreateOrder(createOrderReq)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	// Register open order
	order.ID = oid
	g.openOrders[oid] = order

	return oid, err
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	err := g.api.CancelOrder(order.ID)
	if err != nil {
		return err
	}

	return nil
}

func orderStatusToGatewayState(status string) gateway.OrderState {
	switch status {
	case "queued":
		return gateway.OrderSent
	case "open":
		return gateway.OrderOpen
	case "partially filled":
		return gateway.OrderPartiallyFilled
	case "closed":
		return gateway.OrderClosed
	case "cancelled":
		return gateway.OrderCancelled
	case "completed":
		return gateway.OrderFullyFilled
	default:
		log.Printf("Bitso orderStatusToGatewayState unkown order status [%s]", status)
		return gateway.OrderUnknown
	}
}

func bitsoOrderToGatewayOrder(order ApiOrder, market gateway.Market) gateway.Order {
	return gateway.Order{
		Market:       market,
		ID:           order.ID,
		State:        orderStatusToGatewayState(order.Status),
		Amount:       order.OriginalAmount,
		Price:        order.Price,
		FilledAmount: order.OriginalAmount - order.UnfilledAmount,
		Side:         bitsoSideToGatewaySide(order.Side),
	}
}

func isOpenState(state gateway.OrderState) bool {
	if state == gateway.OrderOpen || state == gateway.OrderPartiallyFilled {
		return true
	}

	return false
}

func isClosedState(state gateway.OrderState) bool {
	if state == gateway.OrderCancelled || state == gateway.OrderClosed || state == gateway.OrderFullyFilled {
		return true
	}

	return false
}

func isOrderUpdate(orderA, orderB gateway.Order) bool {
	if orderA.State != orderB.State || orderA.FilledAmount != orderB.FilledAmount {
		return true
	}

	return false
}

func bitsoSideToGatewaySide(side string) gateway.Side {
	if side == "buy" {
		return gateway.Bid
	}

	return gateway.Ask
}

func gatewaySideToBitsoSide(side gateway.Side) string {
	if side == gateway.Bid {
		return "buy"
	}

	return "sell"
}
