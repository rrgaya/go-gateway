package kucoin

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"regexp"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api        *API
	tickCh     chan gateway.Tick
	openOrders map[string]gateway.Order
}

func NewAccountGateway(api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:        api,
		tickCh:     tickCh,
		openOrders: make(map[string]gateway.Order),
	}
}

func (g *AccountGateway) Connect() error {
	privateWs := true
	ws := NewWsSession(g.api)
	err := ws.Connect(privateWs)
	if err != nil {
		return err
	}

	reqID := strconv.FormatInt(int64(rand.Intn(1000000)), 10)

	ch := make(chan WsResponse, 10)
	unsub := make(chan bool, 1)
	ws.SubscribeMessages(ch, unsub)
	authSuccess := make(chan bool)
	go func() {
		for msg := range ch {
			if msg.Type == "ack" && msg.ID == reqID {
				authSuccess <- true
			}
		}
	}()

	req := WsRequest{
		ID:             reqID,
		Type:           "subscribe",
		Topic:          "/spotMarket/tradeOrders",
		Response:       true,
		PrivateChannel: true,
	}

	err = ws.SendRequest(req)
	if err != nil {
		unsub <- true
		return err
	}

	// Wait for auth
	select {
	case <-authSuccess:
		log.Printf("Authenticated with ws account...")
	case <-time.After(5 * time.Second):
		unsub <- true
		return fmt.Errorf("Auth timeout after 5 seconds...")
	}

	unsub <- true // unsub from previous ch
	ch = make(chan WsResponse, 10)
	ws.SubscribeMessages(ch, nil)

	go g.subscriptionMessageHandler(ch)

	return nil
}

func (g *AccountGateway) subscriptionMessageHandler(ch chan WsResponse) {
	for msg := range ch {
		if handlerIgnoreMsgType(msg.Type) {
			continue
		}

		switch msg.Subject {
		case "orderChange":
			if err := g.processOrderUpdate(msg); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Subject, err.Error())
			}
		default:
			log.Printf("%s account ws - Received unexpected message: id [%s] type [%s] subject [%s] topic [%s] data [%s]", Exchange.Name, msg.ID, msg.Type, msg.Subject, msg.Topic, string(msg.Data))
		}
	}
}

func (g *AccountGateway) processOrderUpdate(msg WsResponse) error {
	update := WsOrder{}
	err := json.Unmarshal(msg.Data, &update)
	if err != nil {
		return fmt.Errorf("%s - Failed to unmarshal order update [%s]. Err: %s", Exchange.Name, string(msg.Data), err)
	}

	order := gateway.Order{
		Market:       gateway.Market{Symbol: update.Symbol},
		ID:           update.OrderID,
		State:        wsOrderToOrderStateGateway(update),
		Side:         sideToGateway(update.Side),
		Price:        update.Price,
		Amount:       update.Size,
		FilledAmount: update.FilledSize,
	}

	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))

	return nil
}

func wsOrderToOrderStateGateway(order WsOrder) gateway.OrderState {
	switch order.Status {
	case "open":
		return gateway.OrderOpen
	case "match":
		return gateway.OrderPartiallyFilled
	case "done":
		if order.Type == "filled" {
			return gateway.OrderFullyFilled
		} else if order.Type == "canceled" {
			return gateway.OrderCancelled
		}
	}
	return gateway.OrderUnknown
}

func sideToGateway(side string) gateway.Side {
	if side == "sell" {
		return gateway.Ask
	} else {
		return gateway.Bid
	}
}

func (g *AccountGateway) normalizeOrderState(order ApiOrder) gateway.OrderState {
	if order.IsActive {
		if order.DealSize > 0 {
			return gateway.OrderPartiallyFilled
		} else {
			return gateway.OrderOpen
		}
	} else {
		return gateway.OrderClosed
	}
}

func (g *AccountGateway) parseToGatewayOrder(order ApiOrder) gateway.Order {
	return gateway.Order{
		Market: gateway.Market{
			Symbol: order.Symbol,
		},
		ID:           order.Id,
		State:        g.normalizeOrderState(order),
		Amount:       order.Size,
		Price:        order.Price,
		FilledAmount: order.DealSize,
		Side:         sideToGateway(order.Side),
	}
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	balances, err := g.api.GetBalances()
	if err != nil {
		return nil, err
	}

	gtwBalances := make([]gateway.Balance, len(balances))

	for i, b := range balances {
		gtwBalances[i] = gateway.Balance{
			Asset:     b.Currency,
			Available: b.Available,
			Total:     b.Balance,
		}
	}

	return gtwBalances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	paramsReq := ApiGetOrdersRequest{
		Symbol: market.Symbol,
	}

	orders, err := g.api.GetOrders(paramsReq)
	if err != nil {
		return nil, err
	}

	gtwOrders := make([]gateway.Order, len(orders))

	for i, order := range orders {
		gtwOrders[i] = g.parseToGatewayOrder(order)
	}

	return gtwOrders, nil
}

var insuficientBalanceMatch = regexp.MustCompile(`Balance insufficient`) // Insufficient balance error code

func (g *AccountGateway) SendOrder(order gateway.Order) (string, error) {
	var side string
	if order.Side == gateway.Bid {
		side = "buy"
	} else {
		side = "sell"
	}

	// Required client order id (not used)
	rid := int64(rand.Intn(10000))
	cid := strconv.FormatInt(time.Now().UnixNano()+rid, 10)

	paramsReq := ApiPostOrderRequest{
		ClientOid: cid,
		Side:      side,
		Symbol:    order.Market.Symbol,
		Type:      "limit",
		Price:     utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
		Size:      utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		PostOnly:  order.PostOnly,
	}

	orderId, err := g.api.PostOrder(paramsReq)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return orderId, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	err := g.api.CancelOrder(order.ID)

	return err
}
