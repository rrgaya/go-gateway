package huobi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api       *API
	accountId int64
	ws        *WsSession
	tickCh    chan gateway.Tick
}

func NewAccountGateway(accountId int64, api *API, ws *WsSession, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:       api,
		accountId: accountId,
		ws:        ws,
		tickCh:    tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	// Subscribe to orders update
	ch := make(chan WsMessage, 0)
	err := g.ws.SubscribeSub("orders#*", ch, nil)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to subscribe to orders updates, err %s", err))
	}

	go g.orderUpdateHandler(ch)

	return nil
}

type OrderUpdate struct {
	Aggressor     bool    `json:"aggressor"`
	ClientOrderID string  `json:"clientOrderId"`
	EventType     string  `json:"eventType"`
	ExecAmt       float64 `json:"execAmt,string"`
	OrderID       int64   `json:"orderId"`
	OrderPrice    float64 `json:"orderPrice,string"`
	OrderSize     float64 `json:"orderSize,string"`
	OrderSource   string  `json:"orderSource"`
	OrderStatus   string  `json:"orderStatus"`
	RemainAmt     float64 `json:"remainAmt,string"`
	Symbol        string  `json:"symbol"`
	TradeID       int64   `json:"tradeId"`
	TradePrice    float64 `json:"tradePrice,string"`
	TradeTime     int64   `json:"tradeTime"`
	TradeVolume   float64 `json:"tradeVolume,string"`
	Type          string  `json:"type"`
}

func (g *AccountGateway) orderUpdateHandler(ch chan WsMessage) {
	for msg := range ch {
		order := OrderUpdate{}
		err := json.Unmarshal(msg.Data, &order)
		if err != nil {
			log.Printf("Huobi failed to unmarshal order update, err: %s", err)
			continue
		}

		g.processOrderUpdate(order)
	}
}

func (g *AccountGateway) processOrderUpdate(order OrderUpdate) {
	o := gateway.Order{}
	o.ID = strconv.FormatInt(order.OrderID, 10)
	o.Price = order.OrderPrice
	o.Amount = order.OrderSize
	o.State = mapAPIOrderStateToCommon(order.OrderStatus)
	o.FilledAmount = order.ExecAmt

	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(o))
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	accountBalance, err := g.api.AccountBalance(g.accountId)
	if err != nil {
		return balances, err
	}

	if accountBalance.State != "working" {
		log.Printf("Huobi notice account balance state is \"%s\", expected to be \"working\"", accountBalance.State)
	}

	positionsMap := make(map[string]*gateway.Balance)
	for _, balance := range accountBalance.List {
		position, ok := positionsMap[balance.Currency]
		if !ok {
			position = &gateway.Balance{
				Asset: strings.ToUpper(balance.Currency),
			}
			positionsMap[balance.Currency] = position
		}

		if balance.Type == "trade" {
			position.Available = balance.Balance
		}

		position.Total += balance.Balance
	}

	balances = make([]gateway.Balance, 0, len(positionsMap))

	for _, position := range positionsMap {
		balances = append(balances, *position)
	}

	return
}

func mapAPIOrderToCommon(o APIOrder) gateway.Order {
	avgPx := 0.0
	if o.FieldAmount > 0 {
		avgPx = o.FieldCashAmount / o.FieldAmount
	}

	return gateway.Order{
		Market: gateway.Market{
			Exchange: Exchange,
			Symbol:   o.Symbol,
		},
		ID:               strconv.FormatInt(o.ID, 10),
		Side:             mapAPIOrderTypeToCommonSide(o.Type),
		State:            mapAPIOrderStateToCommon(o.State),
		Amount:           o.Amount,
		Price:            o.Price,
		FilledAmount:     o.FieldAmount,
		FilledMoneyValue: o.FieldCashAmount,
		AvgPrice:         avgPx,
		Fee:              o.FilledFees,
	}
}

func mapAPIOrderTypeToCommonSide(orderType string) gateway.Side {
	if len(orderType) < 4 {
		log.Printf("Huobi invalid orderType \"%s\", unable to extract side from order type", orderType)
		return ""
	}

	orderSide := orderType[0:4]
	if orderSide == "buy-" {
		return gateway.Bid
	} else if orderSide == "sell" {
		return gateway.Ask
	} else {
		log.Printf("Huobi invalid order side \"%s\"", orderSide)
		return ""
	}
}

func mapAPIOrderStateToCommon(st string) gateway.OrderState {
	switch st {
	case "submitted":
		return gateway.OrderOpen
	case "created":
		return gateway.OrderOpen
	case "filled":
		return gateway.OrderFullyFilled
	case "partial-filled":
		return gateway.OrderPartiallyFilled
	case "canceled":
		return gateway.OrderCancelled
	case "partial-canceled":
		return gateway.OrderCancelled
	}
	return ""
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	openOrders, err := g.api.OpenOrders(g.accountId, market.Symbol)
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
	params := APIPlaceOrder{
		AccountID: g.accountId,
		Symbol:    order.Market.Symbol,
		Amount:    utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		Price:     utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
	}

	if order.Side == gateway.Bid {
		params.Type = "buy-limit"
	} else if order.Side == gateway.Ask {
		params.Type = "sell-limit"
	}

	if order.PostOnly {
		params.Type = params.Type + "-maker"
	}

	return g.api.PlaceOrder(params)
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder(order.ID)
}
