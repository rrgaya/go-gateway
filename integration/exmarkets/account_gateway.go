package exmarkets

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

var insuficientBalanceMatch = regexp.MustCompile(`insufficient balance`)

type AccountGateway struct {
	gateway.BaseAccountGateway
	options         gateway.Options
	markets         []gateway.Market
	api             *API
	openOrders      map[string]gateway.Order
	openOrdersMutex *sync.Mutex
	tickCh          chan gateway.Tick
}

func NewAccountGateway(options gateway.Options, markets []gateway.Market, api *API, tickCh chan gateway.Tick) *AccountGateway {
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
	go g.trackOpenOrders()

	return nil
}

func (g *AccountGateway) subscribeMarketsUpdate(markets []gateway.Market) error {
	for _, market := range markets {
		openOrders, err := g.api.UserOrders(market.Symbol, "open")
		if err != nil {
			return fmt.Errorf("failed to fetch initial open orders [%s], err: %s", market.Symbol, err)
		}

		g.openOrdersMutex.Lock()
		for _, order := range openOrders.Buy {
			g.openOrders[order.ID.String()] = mapApiOpenOrdersToCommon(market, gateway.Bid, order)
		}
		for _, order := range openOrders.Sell {
			g.openOrders[order.ID.String()] = mapApiOpenOrdersToCommon(market, gateway.Ask, order)
		}
		g.openOrdersMutex.Unlock()
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
			openOrders, err := g.api.UserOrders(market.Symbol, "open")
			if err != nil {
				log.Printf("ExMarkets failed to fetch open orders for symbol %s, err: %s", market.Symbol, err)
				continue
			}

			g.openOrdersMutex.Lock()
			for _, ord := range append(openOrders.Buy, openOrders.Sell...) {
				for _, trackedOrder := range g.openOrders {
					if ord.ID.String() == trackedOrder.ID {
						filledAmount := ord.CurrentAmount

						if filledAmount > trackedOrder.FilledAmount {
							trackedOrder.State = gateway.OrderPartiallyFilled
							trackedOrder.FilledAmount = filledAmount
							go g.dispatchOrderUpdate(trackedOrder)
							g.openOrders[trackedOrder.ID] = trackedOrder
						}
					}
				}
			}
			g.openOrdersMutex.Unlock()

			finishedOrders, err := g.api.UserOrders(market.Symbol, "completed")
			if err != nil {
				log.Printf("ExMarkets failed to fetch completed orders for symbol %s, err: %s", market.Symbol, err)
				continue
			}

			g.openOrdersMutex.Lock()
			for _, ord := range append(finishedOrders.Buy, finishedOrders.Sell...) {
				for _, trackedOrder := range g.openOrders {
					if ord.ID.String() == trackedOrder.ID {
						filledAmount := ord.CurrentAmount

						if filledAmount > trackedOrder.FilledAmount {
							trackedOrder.FilledAmount = filledAmount
							if filledAmount >= ord.Amount {
								trackedOrder.State = gateway.OrderFullyFilled
							} else {
								trackedOrder.State = gateway.OrderCancelled
							}
							go g.dispatchOrderUpdate(trackedOrder)
						}

						delete(g.openOrders, trackedOrder.ID)
					}
				}
			}
			g.openOrdersMutex.Unlock()
		}

		time.Sleep(2000 * time.Millisecond)
	}
}

func (g *AccountGateway) dispatchOrderUpdate(order gateway.Order) {
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.WalletsBalance()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0)
	for _, balance := range res {
		balances = append(balances, gateway.Balance{
			Asset:     strings.ToUpper(balance.Currency),
			Total:     balance.Total,
			Available: balance.Available,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	res, err := g.api.UserOrders(market.Symbol, "open")
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, 0)
	for _, resOrder := range res.Buy {
		orders = append(orders, mapApiOpenOrdersToCommon(market, gateway.Bid, resOrder))
	}
	for _, resOrder := range res.Sell {
		orders = append(orders, mapApiOpenOrdersToCommon(market, gateway.Ask, resOrder))
	}

	return orders, nil
}

func mapApiOpenOrdersToCommon(market gateway.Market, side gateway.Side, order APIOrder) gateway.Order {
	filledAmount := order.CurrentAmount
	state := gateway.OrderOpen
	if filledAmount > 0 {
		state = gateway.OrderPartiallyFilled
	}

	return gateway.Order{
		ID:           order.ID.String(),
		Market:       market,
		Side:         side,
		Price:        order.Price,
		Amount:       order.Amount,
		FilledAmount: filledAmount,
		State:        state,
	}
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	var side string
	if order.Side == gateway.Bid {
		side = "buy"
	} else {
		side = "sell"
	}

	resOrder, err := g.api.NewOrder(
		side,
		order.Market.Symbol,
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

	// Register open order
	commonOrder := mapApiOpenOrdersToCommon(order.Market, order.Side, resOrder)
	g.openOrdersMutex.Lock()
	g.openOrders[resOrder.ID.String()] = commonOrder
	g.openOrdersMutex.Unlock()

	return resOrder.ID.String(), nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	resOrder, err := g.api.CancelOrder(order.Market.Symbol, order.ID)
	if err != nil {
		return err
	}

	g.openOrdersMutex.Lock()
	trackedOrder, ok := g.openOrders[resOrder.ID.String()]
	if ok {
		trackedOrder.State = gateway.OrderCancelled
		trackedOrder.FilledAmount = resOrder.CurrentAmount
		go g.dispatchOrderUpdate(trackedOrder)
		delete(g.openOrders, resOrder.ID.String())
	}
	g.openOrdersMutex.Unlock()

	return nil
}
