package bitglobal

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api           *API
	options       gateway.Options
	tickCh        chan gateway.Tick
	trackedOrders map[string]gateway.Order
}

func NewAccountGateway(api *API, options gateway.Options, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:           api,
		options:       options,
		tickCh:        tickCh,
		trackedOrders: make(map[string]gateway.Order),
	}
}

func (g *AccountGateway) Connect() error {
	ws := NewWsSession(g.options)
	err := ws.Connect()
	if err != nil {
		return fmt.Errorf("ws connect err: %s", err)
	}

	err = ws.Authenticate(g.options.ApiKey, g.options.ApiSecret)
	if err != nil {
		return fmt.Errorf("ws authenticate err: %s", err)
	}

	subReq := WsRequest{
		Cmd: "subscribe",
		Args: []interface{}{
			"ORDER",
		},
	}

	// Check for response
	ch := make(chan WsGenericMessage, 10)
	unsub := make(chan bool, 1)
	ws.SubscribeMessages(ch, unsub)
	subSuccess := make(chan bool)
	go func() {
		for msg := range ch {
			if msg.Code == "00001" {
				subSuccess <- true
			}
		}
	}()

	err = ws.SendRequest(subReq)
	if err != nil {
		unsub <- true
		return fmt.Errorf("send sub request err: %s", err)
	}

	// Wait for auth
	select {
	case <-subSuccess:
		log.Printf("Succesfully subbed to account order updates...")
	case <-time.After(5 * time.Second):
		unsub <- true
		return fmt.Errorf("sub timeout after 5 seconds...")
	}

	unsub <- true
	ch = make(chan WsGenericMessage, 10)
	ws.SubscribeMessages(ch, nil)
	go g.subscriptionMessageHandler(ch)

	return nil
}

func (g *AccountGateway) subscriptionMessageHandler(ch chan WsGenericMessage) {
	for msg := range ch {
		switch msg.Topic {
		case "ORDER":
			if err := g.processOrderUpdate(msg); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Topic, err)
			}
		}
	}
}

func mapWsOrderStatus(status string) gateway.OrderState {
	switch status {
	case "created":
		return gateway.OrderOpen
	case "partDeal":
		return gateway.OrderPartiallyFilled
	case "fullDealt":
		return gateway.OrderFullyFilled
	case "canceled":
		return gateway.OrderCancelled
	}

	return gateway.OrderUnknown
}

func (g *AccountGateway) processOrderUpdate(msg WsGenericMessage) error {
	var order WsOrder
	err := json.Unmarshal(msg.Data, &order)
	if err != nil {
		return fmt.Errorf("unmarshal err %s", err)
	}

	status := mapWsOrderStatus(order.Status)

	trackedOrder, _ := g.trackedOrders[order.OID]

	if status == gateway.OrderCancelled || status == gateway.OrderFullyFilled {
		delete(g.trackedOrders, order.OID)
	} else {
		trackedOrder.FilledAmount += order.DealQuantity
		trackedOrder.FilledMoneyValue += order.DealVolume
		g.trackedOrders[order.OID] = trackedOrder
	}

	avgPx := 0.0
	if trackedOrder.FilledAmount > 0 {
		avgPx = trackedOrder.FilledMoneyValue / trackedOrder.FilledAmount
	}

	var eventLogs []gateway.Event

	event := gateway.Event{
		Type: gateway.OrderUpdateEvent,
		Data: gateway.Order{
			ID:               order.OID,
			Price:            order.Price,
			Amount:           order.Quantity,
			State:            status,
			FilledAmount:     trackedOrder.FilledAmount,
			FilledMoneyValue: trackedOrder.FilledMoneyValue,
			AvgPrice:         avgPx,
		},
	}

	eventLogs = append(eventLogs, event)

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.api.SpotAssetsList()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, 0)
	for _, asset := range res {
		balances = append(balances, gateway.Balance{
			Asset:     strings.ToUpper(asset.CoinType),
			Total:     asset.Count + asset.Frozen,
			Available: asset.Count,
		})
	}

	return balances, nil
}

func mapOrderToState(order APIOrder) gateway.OrderState {
	if order.Status == "pending" || order.Status == "send" {
		if order.TradeTotal > 0 {
			return gateway.OrderPartiallyFilled
		} else {
			return gateway.OrderOpen
		}
	}

	if order.Status == "success" || order.Status == "cancel" {
		if order.TradeTotal >= order.Quantity {
			return gateway.OrderFullyFilled
		} else {
			return gateway.OrderCancelled
		}
	}

	return gateway.OrderUnknown
}

func mapOrderSideToGtw(side string) gateway.Side {
	if side == "buy" {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func mapAPIOrderToGtw(order APIOrder) gateway.Order {
	avgPrice, err := order.AvgPrice.Float64()
	if err != nil {
		log.Printf("Failed to parse order %+v avg price, err: %s", order, err)
	}

	return gateway.Order{
		ID:               order.OrderID,
		Side:             mapOrderSideToGtw(order.Side),
		State:            mapOrderToState(order),
		Price:            order.Price,
		Amount:           order.Quantity,
		AvgPrice:         avgPrice,
		FilledAmount:     order.TradeTotal,
		FilledMoneyValue: order.TradeTotal * avgPrice,
	}
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	apiOrders, err := g.api.SpotOpenOrders(market.Symbol)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders = make([]gateway.Order, len(apiOrders))
	for i, record := range apiOrders {
		order := mapAPIOrderToGtw(record)
		order.Market = market
		orders[i] = order
	}

	return orders, nil
}

func mapOrderSideToType(side gateway.Side) string {
	if side == gateway.Bid {
		return "buy"
	} else {
		return "sell"
	}
}

var insuficientBalanceMatch = regexp.MustCompile(`user asset not enough`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)
	params["symbol"] = order.Market.Symbol
	params["type"] = "limit"
	params["side"] = mapOrderSideToType(order.Side)
	params["price"] = utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)
	params["quantity"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)

	res, err := g.api.SpotPlaceOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return res, err
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.SpotCancelOrder(order.Market.Symbol, order.ID)
}
