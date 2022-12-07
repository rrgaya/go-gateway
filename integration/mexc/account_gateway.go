package mexc

import (
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	options gateway.Options
	gateway.BaseAccountGateway
	api    *API
	tickCh chan gateway.Tick
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options: options,
		api:     api,
		tickCh:  tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	return nil
}

func (g *AccountGateway) SubscribeMarkets(markets []gateway.Market) error {
	var refreshInterval time.Duration
	if g.options.RefreshIntervalMs > 0 {
		refreshInterval = time.Duration(g.options.RefreshIntervalMs * int(time.Millisecond))
	} else {
		refreshInterval = 2500 * time.Millisecond
	}

	// Subscribe to market updates
	log.Printf("MEXC polling for %d account trades updates every %v", len(markets), refreshInterval)

	for _, market := range markets {
		go func(market gateway.Market) {
			lastTradeMs := time.Now().UnixMilli() - 100
			trackedTrades := make(map[string]struct{})

			for {
				err := g.updateAccountTrades(market, &lastTradeMs, trackedTrades)
				if err != nil {
					log.Printf("MEXC account data failed to fetch trades %s, err: %s", market.Symbol, err)
					time.Sleep(5 * time.Second)
					continue
				}

				time.Sleep(refreshInterval)
			}
		}(market)
	}

	return nil
}

func (g *AccountGateway) updateAccountTrades(market gateway.Market, lastTradeMs *int64, trackedTrades map[string]struct{}) (err error) {
	params := &url.Values{}
	params.Set("symbol", market.Symbol)
	params.Set("startTime", strconv.FormatInt(*lastTradeMs, 10))

	trades, err := g.api.AccountTrades(params)
	if err != nil {
		return err
	}

	events := make([]gateway.Event, 0)
	var lastTimeMs int64
	for _, trade := range trades {
		if trade.Time > *lastTradeMs {
			// Only track if never tracked
			if _, ok := trackedTrades[trade.ID]; !ok {
				fillEvent := gateway.Fill{
					Symbol:    market.Symbol,
					ID:        trade.ID,
					OrderID:   trade.OrderID,
					Amount:    trade.Qty,
					Price:     trade.Price,
					Timestamp: time.UnixMilli(trade.Time),
				}

				events = append(events, gateway.Event{
					Type: gateway.FillEvent,
					Data: fillEvent,
				})
			}

			lastTimeMs = trade.Time
		}
	}

	if lastTimeMs > 0 {
		*lastTradeMs = lastTimeMs
	}

	g.tickCh <- gateway.Tick{
		EventLog: events,
	}

	return nil
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	accInfo, err := g.api.Account()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, 0)
	for _, balance := range accInfo.Balances {
		balances = append(balances, gateway.Balance{
			Asset:     balance.Asset,
			Available: balance.Free,
			Total:     balance.Free + balance.Locked,
		})
	}

	return balances, err
}

func mapAPIOrderToCommon(o APIOrder) gateway.Order {
	return gateway.Order{
		Market: gateway.Market{
			Exchange: Exchange,
			Symbol:   o.Symbol,
		},
		ID:           o.OrderID,
		Side:         mapAPIOrderSideToCommonSide(o.Side),
		State:        mapAPIOrderStatusToCommon(o.Status),
		Amount:       o.OrigQty,
		Price:        o.Price,
		FilledAmount: o.ExecutedQty,
	}
}

func mapAPIOrderSideToCommonSide(s string) gateway.Side {
	if s == "BUY" {
		return gateway.Bid

	} else {
		return gateway.Ask
	}
}

func mapOrderSideToAPI(s gateway.Side) string {
	if s == gateway.Bid {
		return "BUY"
	} else {
		return "SELL"
	}
}

func mapAPIOrderStatusToCommon(st string) gateway.OrderState {
	switch st {
	case "NEW":
		return gateway.OrderOpen
	case "FIELLD": // Not sure if this status is a typo on the docs
		return gateway.OrderFullyFilled
	case "FILLED":
		return gateway.OrderFullyFilled
	case "PARTIALLY_FILLED":
		return gateway.OrderPartiallyFilled
	case "CANCELED":
		return gateway.OrderCancelled
	case "PARTIALLY_CANCELED":
		return gateway.OrderCancelled
	}
	return ""
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	params := &url.Values{}
	params.Set("symbol", market.Symbol)

	openOrders, err := g.api.OpenOrders(params)
	if err != nil {
		return orders, err
	}

	orders = make([]gateway.Order, 0, len(openOrders))
	for _, order := range openOrders {
		orders = append(orders, mapAPIOrderToCommon(order))
	}

	return
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := &url.Values{}
	params.Set("symbol", order.Market.Symbol)
	params.Set("side", mapOrderSideToAPI(order.Side))
	params.Set("type", "LIMIT")
	params.Set("quantity", utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick))
	params.Set("price", utils.FloatToStringWithTick(order.Price, order.Market.PriceTick))

	res, err := g.api.NewOrder(params)
	if err != nil {
		return "", err
	}

	return res.OrderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	params := &url.Values{}
	params.Set("symbol", order.Market.Symbol)
	params.Set("orderId", order.ID)

	_, err := g.api.CancelOrder(params)
	if err != nil {
		return err
	}

	return nil
}
