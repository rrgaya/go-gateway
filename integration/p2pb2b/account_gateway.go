package p2pb2b

import (
	"fmt"
	"log"
	"regexp"
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
		openOrders, err := g.api.OpenOrders(market.Symbol)
		if err != nil {
			return fmt.Errorf("failed to fetch initial open orders [%s], err: %s", market.Symbol, err)
		}

		g.openOrdersMutex.Lock()
		for _, order := range openOrders {
			g.openOrders[order.ID.String()] = mapApiOpenOrdersToCommon(market, order)
		}
		g.openOrdersMutex.Unlock()
	}

	return nil
}

func (g *AccountGateway) trackOpenOrders() {
	for {
		for key, trackedOrder := range g.openOrders {
			//log.Printf("Checking orders deals for open order %s", trackedOrder.ID)

			deals, err := g.api.OrderDeals(trackedOrder.ID)
			if err != nil {
				log.Printf("P2PB2B failed to fetch order deals for id %s, err: %s", trackedOrder.ID, err)
				continue
			}

			if len(deals) == 0 {
				continue
			}

			filledAmount := 0.0
			filledMoneyValue := 0.0
			for _, d := range deals {
				filledAmount += d.Amount
				filledMoneyValue += d.Amount * d.Price
			}

			//log.Printf("Tracked order filled amount %f", trackedOrder.FilledAmount)
			//log.Printf("Deals total filled amount %f and money value %f", filledAmount, filledMoneyValue)

			if filledAmount > trackedOrder.FilledAmount {
				//log.Printf("Tracked order %s has new execs, order amount: %f, filled amount: %f", trackedOrder.ID, trackedOrder.Amount, filledAmount)

				state := gateway.OrderPartiallyFilled
				if filledAmount >= trackedOrder.Amount {
					state = gateway.OrderFullyFilled
				}

				trackedOrder.State = state
				trackedOrder.FilledAmount = filledAmount
				trackedOrder.FilledMoneyValue = filledMoneyValue
				trackedOrder.AvgPrice = filledMoneyValue / filledAmount
				go g.dispatchOrderUpdate(trackedOrder)
			}

			if trackedOrder.State == gateway.OrderFullyFilled {
				delete(g.openOrders, key)
			}

			time.Sleep(250 * time.Millisecond)
		}

		time.Sleep(2000 * time.Millisecond)
	}
}

func (g *AccountGateway) dispatchOrderUpdate(order gateway.Order) {
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.AccountBalances()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0)
	for asset, balance := range res {
		balances = append(balances, gateway.Balance{
			Asset:     asset,
			Total:     balance.Available + balance.Freeze,
			Available: balance.Available,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	res, err := g.api.OpenOrders(market.Symbol)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, 0)
	for _, resOrder := range res {
		orders = append(orders, mapApiOpenOrdersToCommon(market, resOrder))
	}

	return orders, nil
}

func mapApiOpenOrdersToCommon(market gateway.Market, order APIOrder) gateway.Order {
	filledAmount := order.Amount - order.Left

	var state gateway.OrderState
	if filledAmount >= order.Amount {
		state = gateway.OrderFullyFilled
	} else if filledAmount > 0 {
		state = gateway.OrderPartiallyFilled
	} else {
		state = gateway.OrderOpen
	}

	orderID := order.ID.String()
	if orderID == "" {
		orderID = order.OrderID.String()
	}

	side := gateway.Bid
	if order.Side == "sell" {
		side = gateway.Ask
	}

	return gateway.Order{
		ID:           orderID,
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

	resOrder, err := g.api.CreateOrder(
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
	commonOrder := mapApiOpenOrdersToCommon(order.Market, resOrder)
	g.openOrdersMutex.Lock()
	g.openOrders[commonOrder.ID] = commonOrder
	go g.dispatchOrderUpdate(commonOrder)
	g.openOrdersMutex.Unlock()

	return resOrder.OrderID.String(), nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	resOrder, err := g.api.CancelOrder(order.Market.Symbol, order.ID)
	if err != nil {
		return err
	}

	g.openOrdersMutex.Lock()
	trackedOrder, ok := g.openOrders[resOrder.OrderID.String()]
	if ok {
		trackedOrder.State = gateway.OrderCancelled
		trackedOrder.FilledAmount = resOrder.Amount - resOrder.Left
		go g.dispatchOrderUpdate(trackedOrder)
		delete(g.openOrders, trackedOrder.ID)
	}
	g.openOrdersMutex.Unlock()

	return nil
}
