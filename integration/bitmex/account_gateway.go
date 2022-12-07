package bitmex

import (
	"net/url"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	options gateway.Options
	api     *API
	tickCh  chan gateway.Tick
	ws      *WsSession
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options: options,
		api:     api,
		tickCh:  tickCh,
	}
}

func (g *AccountGateway) Connect(ws *WsSession) error {
	g.ws = ws

	err := g.ws.Authenticate(g.options.ApiKey, g.options.ApiSecret)
	if err != nil {
		return err
	}

	executionsCh := make(chan []WsExecution, 10)

	go func() {
		for execs := range executionsCh {
			for _, exec := range execs {
				g.processExecution(exec)
			}
		}
	}()

	err = g.ws.SubscribeExecutions(executionsCh, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g *AccountGateway) processExecution(exec WsExecution) {
	// Dispatch order update notification
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(gateway.Order{
		ID:               exec.OrderID,
		Market:           gateway.Market{Symbol: exec.Symbol},
		State:            statusToCommon(exec.OrdStatus),
		Price:            exec.Price,
		AvgPrice:         exec.AvgPx,
		Amount:           exec.OrderQty,
		FilledAmount:     exec.CumQty,
		FilledMoneyValue: exec.AvgPx * exec.CumQty,
	}))
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.api.Positions()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, 0, len(res)+1)

	for _, pos := range res {
		balances = append(balances, gateway.Balance{
			Asset:     pos.Symbol,
			Total:     pos.CurrentQty,
			Available: pos.CurrentQty,
		})
	}

	margin, err := g.api.Margin()
	if err != nil {
		return balances, err
	}

	balances = append(balances, gateway.Balance{
		Asset:     "XBT",
		Total:     float64(margin.Amount) / 100000000.0, // Divided by BTC decimal places
		Available: float64(margin.AvailableMargin) / 100000000.0,
	})

	return balances, err
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := url.Values{}

	params.Set("symbol", market.Symbol)
	params.Set("filter", "{\"open\": true}")

	res, err := g.api.GetOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))
	for i, o := range res {
		orders[i] = o.ToCommon()
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	var ordType string
	switch order.Type {
	case gateway.LimitOrder:
		ordType = "Limit"
	case gateway.MarketOrder:
		ordType = "Market"
	}

	var side string
	switch order.Side {
	case gateway.Bid:
		side = "Buy"
	case gateway.Ask:
		side = "Sell"
	}

	var execInst string
	if order.PostOnly {
		execInst = "ParticipateDoNotInitiate"
	}

	px := utils.FloorToTick(order.Price, order.Market.PriceTick)
	amount := utils.FloorToTick(order.Amount, order.Market.AmountTick)

	res, err := g.api.PlaceOrder(order.Market.Symbol, side, ordType, execInst, px, amount)
	if err != nil {
		return "", err
	}

	return res.OrderID, nil
}

func (g *AccountGateway) ReplaceOrder(replaceOrder gateway.Order) (order gateway.Order, err error) {
	req := AmmendOrderReq{}

	if replaceOrder.ID != "" {
		req.OrderID = replaceOrder.ID
	}
	if replaceOrder.ClientOrderID != "" {
		req.OrigClOrdID = replaceOrder.ClientOrderID
	}
	if replaceOrder.NewClientOrderID != "" {
		req.ClOrdID = replaceOrder.NewClientOrderID
	}
	if replaceOrder.Price != 0.0 {
		req.Price = replaceOrder.Price
	}
	if replaceOrder.Amount != 0.0 {
		req.OrderQty = replaceOrder.Amount
	}
	if replaceOrder.RemainingAmount != 0.0 {
		req.LeavesQty = replaceOrder.RemainingAmount
	}

	bitmexOrder, err := g.api.AmmendOrder(req)
	if err != nil {
		return order, err
	}

	return bitmexOrder.ToCommon(), nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	_, err := g.api.CancelOrders([]string{order.ID}, []string{})
	if err != nil {
		return err
	}

	return nil
}
