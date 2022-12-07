package latoken

import (
	"log"
	"regexp"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

var insuficientBalanceMatch = regexp.MustCompile(`enough balance`)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api             *ApiClient
	wsOrderUpdateCh chan OrderUpdate
	currencies      []APICurrency
	tickCh          chan gateway.Tick
}

func NewAccountGateway(api *ApiClient, wsOrderUpdateCh chan OrderUpdate, currencies []APICurrency, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:             api,
		wsOrderUpdateCh: wsOrderUpdateCh,
		currencies:      currencies,
		tickCh:          tickCh,
	}
}

func (g *AccountGateway) Init() error {
	go func() {
		for update := range g.wsOrderUpdateCh {
			g.processOrderUpdate(update)
		}
	}()

	return nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	apiOrders, err := g.api.ActiveOrders(market.Pair.Base, market.Pair.Quote)
	if err != nil {
		return orders, err
	}

	orders = make([]gateway.Order, len(apiOrders))
	for i, o := range apiOrders {
		orders[i] = gateway.Order{
			Market:       market,
			ID:           o.ID,
			State:        mapOrderState(o.Status, o.Quantity, o.Filled),
			Amount:       o.Quantity,
			Price:        o.Price,
			FilledAmount: o.Filled,
			// TODO: LAToken doesn't return the order avg price, since we only use
			// LIMIT orders, most of the time the avg price will be:
			// filled amount * order price
			// But this is not true for all cases, the order may have executed at better prices
			FilledMoneyValue: o.Filled * o.Price,
		}
	}

	return orders, nil
}

func (g *AccountGateway) processOrderUpdate(update OrderUpdate) {
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(gateway.Order{
		Market:       update.Market,
		ID:           update.Order.ID,
		State:        mapWsOrderToState(update.Order),
		Amount:       update.Order.Quantity,
		Price:        update.Order.Price,
		FilledAmount: update.Order.Filled,
		// TODO: LAToken doesn't return the order avg price, since we only use
		// LIMIT orders, most of the time the avg price will be:
		// filled amount * order price
		// But this is not true for all cases, the order may have executed at better prices
		FilledMoneyValue: update.Order.Filled * update.Order.Price,
	}))
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.api.GetBalances()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, 0)
	for _, balance := range res {
		// Only consider spot account balance, not wallet balance
		if balance.Type == "ACCOUNT_TYPE_SPOT" {
			currency, ok := findCurrency(g.currencies, balance.Currency)
			if ok {
				balances = append(balances, gateway.Balance{
					Asset:     currency.Tag,
					Total:     balance.Available + balance.Blocked,
					Available: balance.Available,
				})
			} else {
				log.Printf("LAToken failed to find currency id %s in currencies map while fetching account balances", balance.Currency)
			}
		}
	}

	return balances, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	orderId, err = g.api.CreateOrder(
		order.Market.Pair.Base,
		order.Market.Pair.Quote,
		translateOrderSide(order),
		translateOrderType(order),
		"GOOD_TILL_CANCELLED", // Good till cancel
		utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
		utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
	)

	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return orderId, err
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder(order.ID)
}

func translateOrderSide(order gateway.Order) string {
	if order.Side == gateway.Bid {
		return "BUY"
	} else {
		return "SELL"
	}
}

// Only limit orders are supported for now
func translateOrderType(order gateway.Order) string {
	return "LIMIT"
}

func mapOrderState(status string, total, filled float64) gateway.OrderState {
	if status == OrderPlaced {
		if filled >= total {
			return gateway.OrderFullyFilled
		} else if filled > 0 {
			return gateway.OrderPartiallyFilled
		} else {
			return gateway.OrderOpen
		}
	}

	if status == OrderClosed {
		return gateway.OrderClosed
	}

	if status == OrderCancelled || status == OrderRejected {
		return gateway.OrderCancelled
	}

	return gateway.OrderUnknown
}
