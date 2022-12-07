package gateio

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/gateio/gateapi-go/v6"
	"github.com/herenow/atomic-gtw/gateway"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     *API
	tickCh  chan gateway.Tick
	ws      *WsSession
	options gateway.Options
}

const (
	bidSide = "buy"
	askSide = "sell"

	limitOrderType = "limit"
)

func NewAccountGateway(api *API, tickCh chan gateway.Tick, options gateway.Options) *AccountGateway {
	return &AccountGateway{
		api:     api,
		tickCh:  tickCh,
		options: options,
	}
}

func (g *AccountGateway) Connect(ws *WsSession) error {
	g.ws = ws

	err := g.subscribeOrders()

	return err
}

func (g *AccountGateway) subscribeOrders() error {
	ch := make(chan WsResponse)
	subErrCh := make(chan WsError, 1)
	g.ws.SubscribeMessages(ch, nil)

	go func() {
		for msg := range ch {
			if msg.Channel == "spot.orders" {
				switch msg.Event {
				case "update":
					g.processOrderUpdate(msg)
				case "subscribe":
					subErrCh <- msg.Error
					close(subErrCh)
				}

			}
		}
	}()

	request := WsRequest{
		Time:    time.Now().Unix(),
		Channel: "spot.orders",
		Event:   "subscribe",
		Payload: [1]string{"!all"}, // subscribe to all currency pairs
	}

	g.signRequest(&request)

	g.ws.RequestSubscriptions([]WsRequest{request})

	select {
	case subErr := <-subErrCh:
		if subErr.Code > 0 {
			return fmt.Errorf("Failed to subscribe to order updates, returned ws error code [%d] error msg [%s]", subErr.Code, subErr.Message)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("Failed to subscribe to order updates, timed out after 5 seconds")
	}

	return nil
}

func (g *AccountGateway) processOrderUpdate(msg WsResponse) {
	orders := []WsOrderResult{}
	err := json.Unmarshal(msg.Result, &orders)

	if err != nil {
		log.Printf("Error parsing gateio order update, err: %s, msg.Result: %s", err, string(msg.Result))
		return
	}

	events := make([]gateway.Event, len(orders))

	for i, order := range orders {
		filledAmount := order.Amount - order.AmountLeftToFill

		var state gateway.OrderState
		switch order.Event {
		case "put":
			state = gateway.OrderOpen
		case "update":
			if order.AmountLeftToFill == 0 {
				state = gateway.OrderFullyFilled
			} else {
				state = gateway.OrderPartiallyFilled
			}
		case "finish":
			if order.AmountLeftToFill == 0 {
				state = gateway.OrderFullyFilled
			} else {
				state = gateway.OrderCancelled
			}
		}

		avgPx := 0.0
		if filledAmount > 0 {
			avgPx = order.FilledMoneyValue / filledAmount
		}

		gatewayOrder := gateway.Order{
			ID: order.ID,
			Market: gateway.Market{
				Symbol: order.CurrencyPair,
			},
			State:            state,
			Price:            order.Price,
			Amount:           order.Amount,
			AvgPrice:         avgPx,
			FilledAmount:     filledAmount,
			FilledMoneyValue: order.FilledMoneyValue,
		}

		events[i] = gateway.NewOrderUpdateEvent(gatewayOrder)
	}

	g.tickCh <- gateway.TickWithEvents(events...)

}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	result, err := g.api.FetchBalances()

	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, len(result))
	for i, balance := range result {
		availableBalance := stringToFloat64(balance.Available)
		lockedBalance := stringToFloat64(balance.Locked)

		balances[i] = gateway.Balance{
			Asset:     balance.Currency,
			Total:     availableBalance + lockedBalance,
			Available: availableBalance,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	result, err := g.api.FetchOrders(market.Symbol, "open")
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(result))
	for i, order := range result {
		orders[i] = gateway.Order{
			Market:       market,
			ID:           order.Id,
			State:        orderStatusToGatewayState(order.Status),
			Amount:       stringToFloat64(order.Amount),
			Price:        stringToFloat64(order.Price),
			FilledAmount: stringToFloat64(order.FilledTotal),
			Side:         gateSideToGatewaySide(order.Side),
		}
	}

	return orders, nil
}

var insuficientBalanceMatch = regexp.MustCompile(`BALANCE_NOT_ENOUGH`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	orderReq := gateapi.Order{
		CurrencyPair: order.Market.Symbol,
		Type:         limitOrderType,
		Side:         gatewayOrderSideToGateSide(order.Side),
		Amount:       fmt.Sprintf("%f", order.Amount),
		Price:        fmt.Sprintf("%f", order.Price),
	}

	res, err := g.api.SendOrder(orderReq)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return res.Id, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	_, err := g.api.CancelOrder(order.ID, order.Market.Symbol)
	if err != nil {
		return err
	}

	return nil
}

func (g *AccountGateway) signRequest(req *WsRequest) {
	payload := fmt.Sprintf("channel=%s&event=%s&time=%d", req.Channel, req.Event, req.Time)

	hmac := hmac.New(sha512.New, []byte(g.options.ApiSecret))
	hmac.Write([]byte(payload))

	signature := hex.EncodeToString(hmac.Sum(nil))

	req.Auth = WsAuth{
		Method:    "api_key",
		ApiKey:    g.options.ApiKey,
		Signature: signature,
	}
}
