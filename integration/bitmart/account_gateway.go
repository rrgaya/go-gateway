package bitmart

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	tickCh     chan gateway.Tick
	api        *API
	openOrders map[string]gateway.Order
	options    gateway.Options
	ws         *WsSession
}

func NewAccountGateway(tickCh chan gateway.Tick, api *API, options gateway.Options) *AccountGateway {
	return &AccountGateway{
		tickCh:     tickCh,
		api:        api,
		options:    options,
		openOrders: make(map[string]gateway.Order),
	}
}

func (g *AccountGateway) Connect() error {
	ws := NewWsSession(g.options)
	err := ws.Connect(WsPrivateURL)
	if err != nil {
		return fmt.Errorf("failed to connect to ws, err: %s", err)
	}

	timestamp := time.Now().UnixMilli()
	timestampStr := strconv.FormatInt(timestamp, 10)

	data := fmt.Sprintf("%d#%s#%s", timestamp, g.options.ApiMemo, "bitmart.WebSocket")

	h := hmac.New(sha256.New, []byte(g.options.ApiSecret))
	h.Write([]byte(data))
	sign := hex.EncodeToString(h.Sum(nil))

	req := WsRequest{
		Op:   "login",
		Args: []string{g.options.ApiKey, timestampStr, sign},
	}

	// Check for response
	ch := make(chan WsResponse, 10)
	unsub := make(chan bool, 1)
	ws.SubscribeMessages(ch, unsub)
	authSuccess := make(chan bool)
	go func() {
		for msg := range ch {
			if msg.Event == "login" {
				authSuccess <- true
			}
		}
	}()

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

	g.ws = ws

	return nil
}

func (g *AccountGateway) SubscribeMarkets(markets []gateway.Market) error {
	log.Printf("Requesting order update for %d markets", len(markets))

	args := make([]string, 0)
	for _, market := range markets {
		args = append(args, fmt.Sprintf("spot/user/order:%s", market.Symbol))
	}

	req := WsRequest{
		Op:   "subscribe",
		Args: args,
	}

	err := g.ws.SendRequest(req)
	if err != nil {
		return err
	}

	return nil
}

var orderUpdateRegex = regexp.MustCompile(`spot/user/order`)

func (g *AccountGateway) subscriptionMessageHandler(ch chan WsResponse) {
	for msg := range ch {
		switch {
		case orderUpdateRegex.MatchString(msg.Table):
			if err := g.processOrderUpdate(msg); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Table, err)
			}
		}
	}
}

func (g *AccountGateway) processOrderUpdate(msg WsResponse) error {
	var eventLogs []gateway.Event

	if string(msg.Data) == "" || string(msg.Data) == "\"\"" {
		log.Printf("%s - recevied order update [%+v] with empty data", Exchange.Name, msg)
	}

	updates := []WsOrder{}
	err := json.Unmarshal(msg.Data, &updates)
	if err != nil {
		return fmt.Errorf("%s - Failed to unmarshal order updates [%s]. Err: %s", Exchange.Name, string(msg.Data), err)
	}

	for _, update := range updates {
		st := g.normalizeOrderState(update.State)
		// QuickFix: for some reason, bitmart has a flaw
		// that some order are fully filled but the filled size is
		// comming as empty.
		if update.FilledSize == 0 && st == gateway.OrderFullyFilled {
			log.Printf("BitMart warning: order is [%s] received update of a fully filled order, but filled amount was zero, fixing filled amount...\n%+v", update.OrderID, update)
			update.FilledSize = update.Size
		}

		avgPx := 0.0
		if update.FilledSize > 0 {
			avgPx = update.FilledNotional / update.FilledSize
		}

		event := gateway.Event{
			Type: gateway.OrderUpdateEvent,
			Data: gateway.Order{
				ID:           update.OrderID,
				Price:        update.Price,
				Amount:       update.Size,
				FilledAmount: update.FilledSize,
				AvgPrice:     avgPx,
				State:        st,
			},
		}

		eventLogs = append(eventLogs, event)
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	balances, err := g.api.GetBalances()

	if err != nil {
		return nil, err
	}

	gatewayBalances := make([]gateway.Balance, len(balances))
	for i, balance := range balances {
		gatewayBalances[i] = gateway.Balance{
			Asset:     balance.Currency,
			Total:     balance.Available + balance.Frozen,
			Available: balance.Available,
		}
	}

	return gatewayBalances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	req := ApiGetOrderHistroyRequest{
		Symbol: market.Symbol,
		Status: "9",
		Limit:  100,
	}
	res, err := g.api.GetOrderHistory(req)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))

	for i, order := range res {
		orders[i] = g.parseToGatewayOrder(order)
	}

	return orders, nil
}

var insuficientBalanceMatch = regexp.MustCompile(`Balance not enough`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	var side string
	if order.Side == gateway.Ask {
		side = "sell"
	} else {
		side = "buy"
	}

	apiOrder := ApiPlaceOrder{
		Symbol: order.Market.Symbol,
		Side:   side,
		Type:   "limit",
		Size:   utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		Price:  utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
	}

	if order.PostOnly {
		apiOrder.Type = "limit_maker"
	}

	res, err := g.api.PostOrder(apiOrder)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return strconv.FormatInt(res.OrderId, 10), nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	req := ApiCancelOrderRequest{
		OrderId: order.ID,
	}
	err := g.api.CancelOrder(req)

	return err
}

func (g *AccountGateway) parseToGatewayOrder(order ApiOrder) gateway.Order {
	filledAmount := order.Size - (order.Size - order.UnfilledVolume)
	var side gateway.Side
	if order.Side == "sell" {
		side = gateway.Ask
	} else {
		side = gateway.Bid
	}

	return gateway.Order{
		Market: gateway.Market{
			Symbol: order.Symbol,
		},
		ID:           strconv.FormatInt(order.OrderId, 10),
		State:        g.normalizeOrderState(order.Status),
		Amount:       order.Size,
		Price:        order.Price,
		FilledAmount: filledAmount,
		Side:         side,
	}
}

func (g *AccountGateway) normalizeOrderState(state string) gateway.OrderState {
	switch state {
	case "1", "3", "7", "8":
		return gateway.OrderCancelled
	case "2", "4":
		return gateway.OrderOpen
	case "5":
		return gateway.OrderPartiallyFilled
	case "6":
		return gateway.OrderFullyFilled
	default:
		return gateway.OrderUnknown
	}
}
