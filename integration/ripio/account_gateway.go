package ripio

import (
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	options         gateway.Options
	markets         []gateway.Market
	api             *Api
	openOrders      map[string]gateway.Order
	openOrdersMutex *sync.Mutex
	tickCh          chan gateway.Tick
}

func NewAccountGateway(options gateway.Options, markets []gateway.Market, api *Api, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:         options,
		markets:         markets,
		api:             api,
		openOrders:      make(map[string]gateway.Order),
		openOrdersMutex: &sync.Mutex{},
		tickCh:          tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	// Set initial order states
	err := g.resetOpenOrders()
	if err != nil {
		log.Printf("Ripio failed to fetch open orders, err: %s", err)
		return err
	}

	go g.trackOpenOrders()

	return nil
}

func (g *AccountGateway) resetOpenOrders() error {
	g.openOrdersMutex.Lock()
	defer g.openOrdersMutex.Unlock()

	g.openOrders = make(map[string]gateway.Order)
	for _, market := range g.markets {
		orders, err := g.api.GetOrders(market.Symbol, openOrdersStatuses)
		if err != nil {
			var disabledPairErr = regexp.MustCompile(`Disabled pair`)
			if disabledPairErr.MatchString(err.Error()) {
				log.Printf("Ripio (reset open orders) disabled pair error, err: %s", err)
				continue
			} else {
				return err
			}
		}

		for _, order := range orders {
			g.openOrders[order.OrderID] = mapOrderToCommon(market, order)
		}
	}

	return nil
}

func (g *AccountGateway) marketsWithOpenOrders() []gateway.Market {
	g.openOrdersMutex.Lock()
	defer g.openOrdersMutex.Unlock()

	marketsMap := make(map[string]gateway.Market)
	for _, order := range g.openOrders {
		marketsMap[order.Market.Symbol] = order.Market
	}

	markets := make([]gateway.Market, 0, len(marketsMap))
	for _, market := range marketsMap {
		markets = append(markets, market)
	}

	return markets
}

func (g *AccountGateway) trackOpenOrders() {
	for {
		markets := g.marketsWithOpenOrders()

		for _, market := range markets {
			openOrders, err := g.api.GetOrders(market.Symbol, []string{})
			if err != nil {
				log.Printf("Ripio faied to fetch orders for symbol %s, err: %s", market.Symbol, err)
				continue
			}

			// Diff received open orders with currently tracked open orders
			// Look for changes and dispatch order updates
			g.openOrdersMutex.Lock()
			for _, openOrder := range openOrders {
				for _, trackedOrder := range g.openOrders {
					if openOrder.OrderID == trackedOrder.ID {
						order := mapOrderToCommon(market, openOrder)

						// Check if the currently tracked was updated
						if trackedOrder.State != order.State || trackedOrder.FilledAmount != order.FilledAmount {
							go g.dispatchOrderUpdate(order)
							g.openOrders[openOrder.OrderID] = order
						}

						if order.State == gateway.OrderFullyFilled || order.State == gateway.OrderCancelled {
							delete(g.openOrders, order.ID)
						}
					}
				}

			}
			g.openOrdersMutex.Unlock()
		}

		time.Sleep(1 * time.Second)
	}
}

func (g *AccountGateway) dispatchOrderUpdate(order gateway.Order) {
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.GetBalances()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0, len(res))
	for _, balance := range res {
		balances = append(balances, gateway.Balance{
			Asset:     balance.Symbol,
			Total:     balance.Available + balance.Locked,
			Available: balance.Available,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	res, err := g.api.GetOrders(market.Symbol, openOrdersStatuses)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))
	for index, resOrder := range res {
		orders[index] = mapOrderToCommon(market, resOrder)
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	var side string

	if order.Side == gateway.Bid {
		side = "BUY"
	} else {
		side = "SELL"
	}

	resOrder, err := g.api.CreateOrder(
		order.Market.Symbol,
		side,
		"LIMIT",
		utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
	)
	if err != nil {
		return "", err
	}

	// Register open order
	commonOrder := mapOrderToCommon(order.Market, resOrder)
	g.openOrdersMutex.Lock()
	g.openOrders[resOrder.OrderID] = commonOrder
	g.openOrdersMutex.Unlock()

	return resOrder.OrderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder(order.Market.Symbol, order.ID)
}

func mapOrderToCommon(market gateway.Market, order Order) gateway.Order {
	return gateway.Order{
		Market: market,
		ID:     order.OrderID,
		State:  mapOrderStatusToCommon(order.Status),
		Amount: order.Amount,
		Price:  order.LimitPrice,
		// TODO: Ripio will soon add an AvgPrice attribute that we can use.
		// For now, since we only open "maker" orders, never taker orders
		// we can assume the AvgPrice will be the LimitPrice.
		AvgPrice:         order.LimitPrice,
		FilledAmount:     order.Filled,
		FilledMoneyValue: order.Filled * order.LimitPrice,
	}
}

var openOrderStatus = "OPEN"
var partiallyFilledStatus = "PART"
var fullyFilledStatus = "COMP"
var cancelledStatus = "CANC"
var closedStatus = "CLOS"

var openOrdersStatuses = []string{
	openOrderStatus,
	partiallyFilledStatus,
}

func mapOrderStatusToCommon(status string) gateway.OrderState {
	switch status {
	case openOrderStatus:
		return gateway.OrderOpen
	case partiallyFilledStatus:
		return gateway.OrderPartiallyFilled
	case fullyFilledStatus:
		return gateway.OrderFullyFilled
	case cancelledStatus:
		return gateway.OrderCancelled
	case closedStatus:
		return gateway.OrderCancelled
	default:
		log.Printf("Ripio unable to map order status %s to gateway order state", status)
	}

	return ""
}
