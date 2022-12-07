package hotcoin

import (
	"fmt"
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
	api            *API
	tickCh         chan gateway.Tick
	options        gateway.Options
	trackedMarkets sync.Map
	trackedTxns    sync.Map
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:        options,
		api:            api,
		tickCh:         tickCh,
		trackedMarkets: sync.Map{},
		trackedTxns:    sync.Map{},
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

	log.Printf("Hotcoin polling for account executions every %v", refreshInterval)

	for {
		g.trackedMarkets.Range(func(symbol any, from any) bool {
			g.updateTrackedMarketExecutions(symbol.(string), from.(int64))
			return true
		})

		time.Sleep(refreshInterval)
	}
}

func (g *AccountGateway) updateTrackedMarketExecutions(symbol string, from int64) {
	params := make(map[string]string)
	params["symbol"] = symbol
	if from != 0 {
		params["from"] = strconv.FormatInt(from, 10)
		params["direct"] = "prev"
	}

	res, err := g.api.MatchResults(params)
	if err != nil {
		log.Printf("Failed to get match results for market %s, err: %s", symbol, err)
		return
	}

	events := make([]gateway.Event, 0)
	for _, transaction := range res {
		_, ok := g.trackedTxns.Load(transaction.ID)
		if ok {
			log.Printf("Hotcoin transaction id [%d] of order [%d] was already processed, skipping...", transaction.ID, transaction.OrderID)
			continue
		}

		taker := false
		if transaction.Role == "taker" {
			taker = true
		}

		events = append(events, gateway.NewFillEvent(gateway.Fill{
			ID:        strconv.FormatInt(transaction.ID, 10),
			Symbol:    symbol,
			Timestamp: time.UnixMilli(transaction.CreatedAt),
			OrderID:   strconv.FormatInt(transaction.OrderID, 10),
			Amount:    transaction.FilledAmount,
			Price:     transaction.Price,
			Taker:     taker,
		}))

		if transaction.ID > from {
			from = transaction.ID + 1
		}
	}

	if len(events) > 0 {
		log.Printf("Hotcoin processed %d market executions on market [%s]", len(events), symbol)
		g.trackedMarkets.Store(symbol, from)
		g.tickCh <- gateway.TickWithEvents(events...)
	}

}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.Balance()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0)
	for _, wallet := range res.Wallet {
		balances = append(balances, gateway.Balance{
			Asset:     strings.ToUpper(wallet.Symbol),
			Total:     wallet.Total,
			Available: wallet.Total - wallet.Frozen,
		})
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := make(map[string]string)
	params["type"] = "1"
	params["symbol"] = market.Symbol
	params["count"] = "100"

	resOrders, err := g.api.GetOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, 0)
	for _, resOrder := range resOrders.CurrentOrders {
		state := gateway.OrderOpen
		if resOrder.SuccessAmount > 0 {
			state = gateway.OrderPartiallyFilled
		}
		side := gateway.Ask
		if resOrder.Types == "买单" { // Buy
			side = gateway.Bid
		}

		orders = append(orders, gateway.Order{
			Market:           market,
			ID:               strconv.FormatInt(resOrder.ID, 10),
			Side:             side,
			State:            state,
			Amount:           resOrder.Count,
			Price:            resOrder.Price,
			FilledAmount:     resOrder.SuccessAmount,
			FilledMoneyValue: 0,
			AvgPrice:         0,
		})
	}

	return orders, nil
}

// Inssuficient balance err code
var insuficientBalanceMatch = regexp.MustCompile(`10118`)
var orderValueTooSmallMatch = regexp.MustCompile(`交易金额不得小于`)
var orderSizeTooSmallMatch = regexp.MustCompile(`必须大于`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)

	if order.Side == gateway.Bid {
		params["type"] = "buy"
	} else {
		params["type"] = "sell"
	}

	params["symbol"] = order.Market.Symbol
	params["tradeAmount"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
	params["tradePrice"] = utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)

	res, err := g.api.CreateOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		case orderValueTooSmallMatch.MatchString(err.Error()):
			return "", gateway.MinOrderSizeErr
		case orderSizeTooSmallMatch.MatchString(err.Error()):
			return "", gateway.MinOrderSizeErr
		default:
			return "", err
		}
	}

	orderID := strconv.FormatInt(res.ID, 10)
	if orderID == "" {
		return "", fmt.Errorf("empty order id")
	}

	g.trackedMarkets.LoadOrStore(order.Market.Symbol, int64(0))

	return orderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	err := g.api.CancelOrder(order.ID)
	if err != nil {
		return err
	}

	return nil
}
