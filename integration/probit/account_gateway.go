package probit

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	options         gateway.Options
	api             *API
	ws              *WsSession
	tickCh          chan gateway.Tick
	openOrders      map[string]gateway.Order
	openOrdersMutex *sync.RWMutex
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

	token, err := g.api.Token()
	if err != nil {
		return err
	}

	err = g.ws.Authenticate(token.AccessToken)
	if err != nil {
		return err
	}

	err = g.subOpenOrders()
	if err != nil {
		return err
	}

	return nil
}

func (g *AccountGateway) subOpenOrders() error {
	ch := make(chan WsMessage, 1)
	err := g.ws.SubscribeOpenOrder(ch, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range ch {
			if msg.Reset {
				// Ignore, this is a snapshot of the open orders
				continue
			}

			var orders []APIOrder
			err := json.Unmarshal(msg.Data, &orders)
			if err != nil {
				log.Printf("ProBit failed to unmarshal ws open orders, data:\n%s\nerr: %s", string(msg.Data), err)
				continue
			}

			for i := range orders {
				order := apiOrderToCommon(orders[i])
				g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
			}
		}
	}()

	return nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	apiOrders, err := g.api.OpenOrder(market.Symbol)
	if err != nil {
		return orders, err
	}

	orders = make([]gateway.Order, len(apiOrders))
	for i, o := range apiOrders {
		orders[i] = apiOrderToCommon(o)
	}

	return orders, nil
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.api.Balance()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, len(res))
	for i, balance := range res {
		balances[i] = gateway.Balance{
			Asset:     balance.CurrencyID,
			Total:     balance.Total,
			Available: balance.Available,
		}
	}

	return balances, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	res, err := g.api.NewOrder(APINewOrder{
		MarketID:    order.Market.Symbol,
		LimitPrice:  utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
		Quantity:    utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		Side:        translateOrderSide(order),
		TimeInForce: "gtc",
		Type:        "limit",
	})

	if err != nil && strings.Contains(err.Error(), notEnoughtBalanceErrMatch) {
		return "", gateway.InsufficientBalanceErr
	}

	return res.ID, err
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	_, err := g.api.CancelOrder(APICancelOrder{
		MarketID: order.Market.Symbol,
		OrderID:  order.ID,
	})

	return err
}

func translateOrderSide(order gateway.Order) string {
	if order.Side == gateway.Bid {
		return "buy"
	} else {
		return "sell"
	}
}

func mapOrderSideToCommon(side string) gateway.Side {
	if side == "buy" {
		return gateway.Bid
	}
	return gateway.Ask
}

func mapOrderStatusToCommon(status string, leftAmount float64) gateway.OrderState {
	switch status {
	case "open":
		return gateway.OrderOpen
	case "filled":
		if leftAmount <= 0 {
			return gateway.OrderFullyFilled
		} else {
			return gateway.OrderPartiallyFilled
		}
	case "cancelled":
		return gateway.OrderCancelled
	}
	return ""
}
