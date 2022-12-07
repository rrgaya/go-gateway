package coinstore

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
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
	trackedMarkets      map[string]TrackedMarket
	trackedMarketsMutex *sync.Mutex
}

type TrackedMarket struct {
	ErrCount       int
	OpenOrders     map[string]struct{}
	TrackedMatches map[string]struct{}
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:             options,
		api:                 api,
		tickCh:              tickCh,
		trackedMarkets:      make(map[string]TrackedMarket),
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

	log.Printf("CoinStore polling for account executions every %v", refreshInterval)

	for {
		trackedSymbols := make([]string, 0)
		g.trackedMarketsMutex.Lock()
		for symbol, _ := range g.trackedMarkets {
			trackedSymbols = append(trackedSymbols, symbol)
		}
		g.trackedMarketsMutex.Unlock()

		for _, symbol := range trackedSymbols {
			var trackedMarket TrackedMarket
			g.trackedMarketsMutex.Lock()
			trackedMarket = g.trackedMarkets[symbol]
			g.trackedMarketsMutex.Unlock()

			params := make(map[string]string)
			params["symbol"] = symbol

			res, err := g.api.AccountMatches(params)
			if err != nil {
				trackedMarket.ErrCount += 1
				if trackedMarket.ErrCount > 10 {
					err = fmt.Errorf("Tracker market [%s] after [%d] fetch account tries, still receiving err [%s]", symbol, trackedMarket.ErrCount, err)
					panic(err)
				}

				log.Printf("Failed to fetch account matches [%s], retrying in 3 seconds... err: %s", symbol, err)
				time.Sleep(3 * time.Second)
				continue
			}

			events := make([]gateway.Event, 0)
			for _, match := range res {
				orderID := strconv.FormatInt(match.OrderID, 10)
				matchID := strconv.FormatInt(match.ID, 10)

				// Check if we are tracking this order
				if _, ok := trackedMarket.OpenOrders[orderID]; !ok {
					continue
				}

				// Check if this match was already processed
				if _, ok := trackedMarket.TrackedMatches[matchID]; ok {
					continue
				}

				var side gateway.Side
				if match.Side > 0 {
					side = gateway.Bid
				} else {
					side = gateway.Ask
				}

				fillEvent := gateway.Fill{
					ID:      matchID,
					OrderID: orderID,
					Symbol:  symbol,
					Side:    side,
					Amount:  match.ExecQty,
					Price:   match.ExecAmt / match.ExecQty,
				}

				events = append(events, gateway.Event{
					Type: gateway.FillEvent,
					Data: fillEvent,
				})

				// Register match as tracked to avoid duplicate tracking
				trackedMarket.TrackedMatches[matchID] = struct{}{}
			}

			if len(events) > 0 {
				g.tickCh <- gateway.Tick{
					EventLog: events,
				}
			}

			trackedMarket.ErrCount = 0

			g.trackedMarketsMutex.Lock()
			g.trackedMarkets[symbol] = trackedMarket
			g.trackedMarketsMutex.Unlock()
		}

		time.Sleep(refreshInterval)
	}
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.AssetList()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balanceMap := make(map[string]gateway.Balance)

	for _, asset := range res {
		bal := balanceMap[asset.Currency]

		switch asset.TypeName {
		case "AVAILABLE":
			bal.Available += asset.Balance
			bal.Total += asset.Balance
		case "FROZEN":
			bal.Total += asset.Balance
		}

		balanceMap[asset.Currency] = bal
	}

	balances := make([]gateway.Balance, len(balanceMap))
	for currency, bal := range balanceMap {
		balances = append(balances, gateway.Balance{
			Asset:     currency,
			Total:     bal.Total,
			Available: bal.Available,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := make(map[string]string)
	params["symbol"] = market.Symbol

	res, err := g.api.CurrentOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, 0)
	for _, resOrder := range res {
		if resOrder.Symbol == market.Symbol {
			state := gateway.OrderOpen
			if resOrder.CumQty > 0 {
				state = gateway.OrderPartiallyFilled
			}

			avgPx := 0.0
			if resOrder.CumQty > 0 {
				avgPx = resOrder.CumAmt / resOrder.CumQty
			}

			orders = append(orders, gateway.Order{
				Market:           market,
				ID:               resOrder.OrdID,
				State:            state,
				Amount:           resOrder.OrdQty,
				Price:            resOrder.OrdPrice,
				FilledAmount:     resOrder.CumQty,
				FilledMoneyValue: resOrder.CumAmt,
				AvgPrice:         avgPx,
			})
		}
	}

	return orders, nil
}

var insuficientBalanceMatch = regexp.MustCompile(`Insufficient quantity available`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)

	params["symbol"] = order.Market.Symbol

	if order.Side == gateway.Bid {
		params["side"] = "BUY"
	} else {
		params["side"] = "SELL"
	}

	if order.Type == gateway.MarketOrder {
		params["ordType"] = "MARKET"
	} else if order.PostOnly {
		params["ordType"] = "POST_ONLY"
	} else {
		params["ordType"] = "LIMIT"
	}

	params["timeInForce"] = "GTC"
	params["ordQty"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
	params["ordPrice"] = utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)

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
		g.trackedMarkets[sym] = TrackedMarket{
			OpenOrders:     make(map[string]struct{}),
			TrackedMatches: make(map[string]struct{}),
		}
	}
	g.trackedMarkets[sym].OpenOrders[orderID] = struct{}{}
	g.trackedMarketsMutex.Unlock()

	return orderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	params := make(map[string]string)

	params["symbol"] = order.Market.Symbol
	params["ordId"] = order.ID

	res, err := g.api.CancelOrder(params)
	if err != nil {
		return err
	}

	if res.State != "CANCELED" && res.State != "FILLED" {
		return fmt.Errorf("expected order state to be CANCELED or FILLED after cancel, instead was: %s", res.State)
	}

	return nil
}
