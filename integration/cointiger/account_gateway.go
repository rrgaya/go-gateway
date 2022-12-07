package cointiger

import (
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api                 *API
	tickCh              chan gateway.Tick
	options             gateway.Options
	trackedMarkets      map[string]struct{}
	oldestMatchID       int64
	trackedMarketsMutex *sync.Mutex
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:             options,
		api:                 api,
		tickCh:              tickCh,
		trackedMarkets:      make(map[string]struct{}),
		trackedMarketsMutex: &sync.Mutex{},
	}
}

func (g *AccountGateway) Connect() error {
	go g.trackAccountExecutions()

	return nil
}

func (g *AccountGateway) trackAccountExecutions() {
	var refreshInterval time.Duration
	if g.options.RefreshIntervalMs > 0 {
		refreshInterval = time.Duration(g.options.RefreshIntervalMs * int(time.Millisecond))
	} else {
		refreshInterval = 2500 * time.Millisecond
	}

	log.Printf("CoinTiger polling for account executions every %v", refreshInterval)

	for {
		trackedSymbols := make([]string, 0)
		g.trackedMarketsMutex.Lock()
		for symbol, _ := range g.trackedMarkets {
			trackedSymbols = append(trackedSymbols, symbol)
		}
		g.trackedMarketsMutex.Unlock()

		if len(trackedSymbols) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		params := make(map[string]string)
		params["symbol"] = strings.Join(trackedSymbols, ",")
		params["from"] = strconv.FormatInt(g.oldestMatchID, 10)

		res, err := g.api.Transactions(params)
		if err != nil {
			log.Printf("Failed to fetch account transactions, err: %s", err)
			continue
		}

		events := make([]gateway.Event, 0)
		for _, match := range res {
			if match.ID > g.oldestMatchID {
				orderID := strconv.FormatInt(match.OrderID, 10)

				var side gateway.Side
				if match.Type == "buy-limit" || match.Type == "buy-market" {
					side = gateway.Bid
				} else {
					side = gateway.Ask
				}

				fillEvent := gateway.Fill{
					ID:      strconv.FormatInt(match.ID, 10),
					OrderID: orderID,
					Symbol:  match.Symbol,
					Side:    side,
					Amount:  match.Volume,
					Price:   match.Price,
				}

				events = append(events, gateway.Event{
					Type: gateway.FillEvent,
					Data: fillEvent,
				})

				g.oldestMatchID = match.ID
			}
		}

		if len(events) > 0 {
			g.tickCh <- gateway.Tick{
				EventLog: events,
			}
		}

		time.Sleep(refreshInterval)
	}
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.Balances()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0)
	for _, bal := range res {
		balances = append(balances, gateway.Balance{
			Asset:     strings.ToUpper(bal.Coin),
			Total:     bal.Normal + bal.Lock,
			Available: bal.Normal,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := make(map[string]string)
	params["symbol"] = market.Symbol
	params["states"] = "new,part_filled"

	res, err := g.api.Orders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, 0)
	for _, resOrder := range res {
		if resOrder.Symbol == market.Symbol {
			state := gateway.OrderOpen
			if resOrder.DealVolume > 0 {
				state = gateway.OrderPartiallyFilled
			}

			orders = append(orders, gateway.Order{
				Market:           market,
				ID:               strconv.FormatInt(resOrder.ID, 10),
				State:            state,
				Amount:           resOrder.Volume,
				Price:            resOrder.Price,
				FilledAmount:     resOrder.DealVolume,
				FilledMoneyValue: resOrder.DealMoney,
				AvgPrice:         resOrder.AvgPrice,
			})
		}
	}

	return orders, nil
}

var insuficientBalanceMatch = regexp.MustCompile(`Balance insufficient`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)

	if order.Side == gateway.Bid {
		params["side"] = "BUY"
	} else {
		params["side"] = "SELL"
	}

	if order.Type == gateway.MarketOrder {
		params["type"] = "2"
	} else {
		params["type"] = "1" // Limit
	}

	params["symbol"] = order.Market.Symbol
	params["volume"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
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

	orderID := strconv.FormatInt(res.OrderID, 10)

	g.trackedMarketsMutex.Lock()
	sym := order.Market.Symbol
	if _, ok := g.trackedMarkets[sym]; !ok {
		g.trackedMarkets[sym] = struct{}{}
	}
	g.trackedMarketsMutex.Unlock()

	return orderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	params := make(map[string]string)
	params["symbol"] = order.Market.Symbol
	params["order_id"] = order.ID

	err := g.api.CancelOrder(params)
	if err != nil {
		return err
	}

	return nil
}
