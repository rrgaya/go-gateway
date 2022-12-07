package digitra

import (
	"fmt"
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
	tickCh             chan gateway.Tick
	options            gateway.Options
	trackedOrders      map[string]struct{}
	trackedOrdersMutex *sync.Mutex
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:            options,
		api:                api,
		tickCh:             tickCh,
		trackedOrders:      make(map[string]struct{}),
		trackedOrdersMutex: &sync.Mutex{},
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

	log.Printf("Digitra polling for account executions every %v", refreshInterval)

	for {
		trackedOrders := make([]string, 0)
		g.trackedOrdersMutex.Lock()
		for id, _ := range g.trackedOrders {
			trackedOrders = append(trackedOrders, id)
		}
		g.trackedOrdersMutex.Unlock()

		if len(trackedOrders) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if len(trackedOrders) > 20 {
			log.Printf("WARNING: Digitra tracking [%d] open orders", len(trackedOrders))
		}

		for _, orderID := range trackedOrders {
			resOrder, err := g.api.GetOrder(orderID, nil)
			if err != nil {
				log.Printf("Failed to get order %s, err: %s", orderID, err)
				continue
			}

			order := gateway.Order{
				ID:               orderID,
				State:            apiOrderStatusToGtw(resOrder.Status),
				Amount:           resOrder.Size,
				Price:            resOrder.Price,
				FilledAmount:     resOrder.Filled,
				FilledMoneyValue: resOrder.Filled * resOrder.FilledWeightedPrice,
				AvgPrice:         resOrder.FilledWeightedPrice,
			}

			g.tickCh <- gateway.TickWithEvents(
				gateway.NewOrderUpdateEvent(order),
			)

			// Final status, don't expect new order updates
			if order.State != gateway.OrderOpen && order.State != gateway.OrderPartiallyFilled {
				g.trackedOrdersMutex.Lock()
				delete(g.trackedOrders, orderID)
				g.trackedOrdersMutex.Unlock()
			}
		}

		time.Sleep(refreshInterval)
	}
}

func apiOrderStatusToGtw(status string) gateway.OrderState {
	switch status {
	case "OPEN":
		return gateway.OrderOpen
	case "PENDING_CANCELING":
		return gateway.OrderOpen
	case "CANCELED":
		return gateway.OrderCancelled
	case "CANCELED_PENDING_BALANCE":
		return gateway.OrderCancelled
	case "FILLED":
		return gateway.OrderFullyFilled
	case "PARTIALLY_FILLED":
		return gateway.OrderPartiallyFilled
	}

	return gateway.OrderUnknown
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.Balances()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0)
	for _, bal := range res {
		balances = append(balances, gateway.Balance{
			Asset:     strings.ToUpper(bal.Asset),
			Total:     bal.Amount + bal.AmountTrading,
			Available: bal.Amount,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := make(map[string]interface{})
	params["status"] = "OPEN"
	params["page_size"] = "100"

	openOrders, err := g.api.GetOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	params["status"] = "PARTIALLY_FILLED"
	partiallyFilledOrders, err := g.api.GetOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	resOrders := append(openOrders, partiallyFilledOrders...)

	orders := make([]gateway.Order, 0)
	for _, resOrder := range resOrders {
		if resOrder.Market == market.Symbol {
			state := gateway.OrderOpen
			if resOrder.Filled > 0 {
				state = gateway.OrderPartiallyFilled
			}

			orders = append(orders, gateway.Order{
				Market:           market,
				ID:               resOrder.ID,
				State:            state,
				Amount:           resOrder.Size,
				Price:            resOrder.Price,
				FilledAmount:     resOrder.Filled,
				FilledMoneyValue: resOrder.Filled * resOrder.FilledWeightedPrice,
				AvgPrice:         resOrder.FilledWeightedPrice,
			})
		}
	}

	return orders, nil
}

var insuficientBalanceMatch = regexp.MustCompile(`insufficient_balance`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]interface{})

	if order.Side == gateway.Bid {
		params["side"] = "BUY"
	} else {
		params["side"] = "SELL"
	}

	params["type"] = "LIMIT"
	params["market"] = order.Market.Symbol
	params["size"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
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

	orderID := res.ID
	if orderID == "" {
		return "", fmt.Errorf("empty order id")
	}

	g.trackedOrdersMutex.Lock()
	g.trackedOrders[orderID] = struct{}{}
	g.trackedOrdersMutex.Unlock()

	return orderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	err := g.api.CancelOrder(order.ID)
	if err != nil {
		return err
	}

	return nil
}
