package lbank

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     *API
	tickCh  chan gateway.Tick
	options gateway.Options
}

func NewAccountGateway(api *API, tickCh chan gateway.Tick, options gateway.Options) *AccountGateway {
	return &AccountGateway{
		api:     api,
		tickCh:  tickCh,
		options: options,
	}
}

func (g *AccountGateway) Connect() error {
	ws := NewWsSession(g.options)
	err := ws.Connect()
	if err != nil {
		return err
	}

	// Get WS sub key
	subKey, err := g.api.SubKey()
	if err != nil {
		return fmt.Errorf("failed to get api SubKey, err: %s", err)
	}

	log.Printf("LBank ws sub key [%s]", subKey)

	err = ws.WriteMessage(
		[]byte("{\"action\": \"subscribe\", \"subscribe\": \"orderUpdate\", \"subscribeKey\": \"" + subKey + "\", \"pair\": \"all\"}"),
	)
	if err != nil {
		return fmt.Errorf("failed write subkey msg: %s", err)
	}

	ch := make(chan WsGenericMessage, 10)
	ws.SubscribeMessages(ch, nil)

	go g.wsMessageHandler(ch)

	return nil
}

func (g *AccountGateway) wsMessageHandler(ch chan WsGenericMessage) {
	for msg := range ch {
		switch msg.Type {
		case "orderUpdate":
			g.processOrderUpdate(msg)
		}
	}
}

type WsOrderUpdateMsg struct {
	OrderUpdate WsOrderUpdate `json:"orderUpdate"`
}

type WsOrderUpdate struct {
	AccAmt      float64 `json:"accAmt,string"`
	Amount      string  `json:"amount"`
	AvgPrice    float64 `json:"avgPrice,string"`
	OrderAmt    float64 `json:"orderAmt,string"`
	OrderPrice  float64 `json:"orderPrice,string"`
	OrderStatus int     `json:"orderStatus"`
	Symbol      string  `json:"symbol"`
	Type        string  `json:"type"`
	UUID        string  `json:"uuid"`
}

func (g *AccountGateway) processOrderUpdate(wsMsg WsGenericMessage) {
	msg := WsOrderUpdateMsg{}
	err := json.Unmarshal(wsMsg.Data, &msg)
	if err != nil {
		log.Printf("Failed to processOrderUpdate json.Unmarshal [%s], err: %s", string(wsMsg.Data), err)
		return
	}

	update := msg.OrderUpdate
	order := gateway.Order{
		Market:       gateway.Market{Symbol: update.Symbol},
		ID:           update.UUID,
		State:        statusToOrderStateGateway(update.OrderStatus),
		Side:         typeToSideGateway(update.Type),
		Price:        update.OrderPrice,
		Amount:       update.OrderAmt,
		FilledAmount: update.AccAmt,
		AvgPrice:     update.AvgPrice,
	}

	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	result, err := g.api.AssetInformation()

	if err != nil {
		return []gateway.Balance{}, err
	}

	balancesMap := make(map[string]gateway.Balance)
	appendToBalanceMap := func(sym string, total, available json.Number) {
		bal, _ := balancesMap[sym]

		t, _ := total.Float64()
		a, _ := total.Float64()

		bal.Asset = strings.ToUpper(sym)
		bal.Total += t
		bal.Available += a
		balancesMap[sym] = bal
	}

	for k, total := range result.Asset {
		appendToBalanceMap(k, total, "")
	}
	for k, available := range result.Free {
		appendToBalanceMap(k, "", available)
	}

	balances := make([]gateway.Balance, 0)
	for _, balance := range balancesMap {
		balances = append(balances, balance)
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := url.Values{}
	params.Set("symbol", market.Symbol)
	params.Set("current_page", "1")
	params.Set("page_length", "200")

	result, err := g.api.PendingOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(result.Orders))
	for i, order := range result.Orders {
		o := apiOrderToGateway(order)
		o.Market = market
		orders[i] = o
	}

	return orders, nil
}

func apiOrderToGateway(order APIOrder) gateway.Order {
	return gateway.Order{
		ID:           order.OrderID,
		State:        statusToOrderStateGateway(order.Status),
		Side:         typeToSideGateway(order.Type),
		Price:        order.Price,
		Amount:       order.Amount,
		FilledAmount: order.DealAmount,
		AvgPrice:     order.AvgPrice,
	}
}

var insuficientBalanceMatch = regexp.MustCompile(`10016`) // Insufficient balance error code

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := url.Values{}
	params.Set("symbol", order.Market.Symbol)
	params.Set("amount", utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick))
	params.Set("price", utils.FloatToStringWithTick(order.Price, order.Market.PriceTick))

	if order.Side == gateway.Bid {
		if order.PostOnly {
			params.Set("type", "buy_maker")
		} else {
			params.Set("type", "buy")
		}
	} else if order.Side == gateway.Ask {
		if order.PostOnly {
			params.Set("type", "sell_maker")
		} else {
			params.Set("type", "sell")
		}
	}

	res, err := g.api.CreateOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return res.OrderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	params := url.Values{}
	params.Set("symbol", order.Market.Symbol)
	params.Set("order_id", order.ID)

	_, err := g.api.CancelOrder(params)
	if err != nil {
		return err
	}

	return nil
}
