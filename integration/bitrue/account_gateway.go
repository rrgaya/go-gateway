package bitrue

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     *API
	tickCh  chan gateway.Tick
	options gateway.Options
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options: options,
		api:     api,
		tickCh:  tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	// Start user data stream, to receive account updates
	res, err := g.api.NewListenKey()
	if err != nil {
		return err
	}

	if res.ListenKey == "" {
		return fmt.Errorf("stream api returned blank listen key")
	}

	streamUri := fmt.Sprintf(StreamWsUrl, res.ListenKey)
	ws := utils.NewWsClient()
	err = ws.Connect(streamUri)
	if err != nil {
		return fmt.Errorf("stream ws conn err: %s", err)
	}

	log.Printf("Connected to stream ws [%s]...", streamUri)

	// Sub to order updates
	if err := ws.WriteMessage([]byte("{\"event\":\"sub\",\"params\":{\"channel\":\"user_order_update\"}}")); err != nil {
		return fmt.Errorf("failed write order update sub msg to ws: %s", err)

	}

	ch := make(chan []byte, 100)
	orderUpdateSubCh := make(chan error, 1)
	ws.SubscribeMessages(ch)

	go g.messageHandler(ch, orderUpdateSubCh)

	// Wait for order sub confirmation
	select {
	case err := <-orderUpdateSubCh:
		if err != nil {
			return fmt.Errorf("order update sub returned err: %s", err)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for ws order update sub confirmation...")
	}

	// Keepalive the stream, send a ping every 30m
	go func() {
		for {
			time.Sleep(15 * time.Minute)

			err := g.api.RenewListenKey(res.ListenKey)
			if err != nil {
				log.Printf("Failed to send keepalive request to ws user data stream, error %s", err)
				log.Printf("Retrying keepalive request in 10 seconds...")

				time.Sleep(10 * time.Second)

				err := g.api.RenewListenKey(res.ListenKey)
				if err != nil {
					panic(fmt.Errorf("unable to renew listen key [%s]", res.ListenKey))
				}
			}
		}
	}()

	return nil
}

func (g *AccountGateway) messageHandler(ch chan []byte, orderUpdateSub chan error) {
	for data := range ch {
		var genMsg WsStreamMessage
		err := json.Unmarshal(data, &genMsg)
		if err != nil {
			log.Printf("Bitrue messageHandler failed to unmarshal genMsg, err: %s, data: %s", err, string(data))
			continue
		}

		if genMsg.Event != "" {
			switch genMsg.Event {
			case "user_order_update":
				if genMsg.Status == "ok" {
					orderUpdateSub <- nil
				} else {
					orderUpdateSub <- fmt.Errorf("user_order_update sub failed, err msg [%s]", string(data))
				}
			}
		} else {
			var orderUpdate WsOrderUpdate
			err := json.Unmarshal(data, &orderUpdate)
			if err != nil {
				log.Printf("Bitrue messageHandler failed to unmarshal orderUpdate, err: %s, data: %s", err, string(data))
				continue
			}

			switch orderUpdate.EventType {
			case "ORDER":
				g.processOrderUpdate(orderUpdate)
			}
		}

		if genMsg.Status == "error" {
			log.Printf("market data ws err msg: %s", string(data))
		}
	}
}

type WsStreamMessage struct {
	Event  string `json:"event"`
	Status string `json:"status"`
}

type WsOrderUpdate struct {
	EventType                string  `json:"e"`
	Symbol                   string  `json:"s"`        // Symbol
	Side                     int64   `json:"S"`        // Side
	Type                     int64   `json:"o"`        // Order type
	TimeInForce              string  `json:"f"`        // Time in force
	Quantity                 float64 `json:"q,string"` // Order quantity
	Price                    float64 `json:"p,string"` // Order price
	OrderStatus              int64   `json:"X"`        // Current order status
	OrderEvent               int64   `json:"x"`        // Current execution type
	OrderID                  int64   `json:"i"`        // Order ID
	EventID                  int64   `json:"I"`        // Event ID
	CumulativeFilledQuantity float64 `json:"z,string"` // Cumulative filled quantity
	CumulativeFilledQuote    float64 `json:"Y,string"` // Cumulative quote asset transacted quantity
	EventTime                int64   `json:"E"`        // Event time
}

func (g *AccountGateway) processOrderUpdate(update WsOrderUpdate) {
	events := make([]gateway.Event, 0, 2)

	symbol := strings.ToUpper(update.Symbol)

	avgPx := 0.0
	if update.CumulativeFilledQuantity > 0 {
		avgPx = update.CumulativeFilledQuote / update.CumulativeFilledQuantity
	}

	events = append(events, gateway.NewOrderUpdateEvent(gateway.Order{
		ID:               strconv.FormatInt(update.OrderID, 10),
		Market:           gateway.Market{Symbol: symbol},
		State:            wsOrderToState(update),
		Price:            update.Price,
		Amount:           update.Quantity,
		AvgPrice:         avgPx,
		FilledAmount:     update.CumulativeFilledQuantity,
		FilledMoneyValue: update.CumulativeFilledQuote,
	}))

	g.tickCh <- gateway.TickWithEvents(events...)
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.AccountInfo()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, len(res.Balances))

	for index, balance := range res.Balances {
		balances[index] = gateway.Balance{
			Asset:     strings.ToUpper(balance.Asset),
			Available: balance.Free,
			Total:     balance.Free + balance.Locked,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := make(map[string]string)
	params["symbol"] = market.Symbol

	res, err := g.api.OpenOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))

	for index, resOrder := range res {
		state := gateway.OrderOpen
		if resOrder.ExecutedQty > 0 {
			state = gateway.OrderPartiallyFilled
		}

		avgPx := 0.0
		if resOrder.ExecutedQty > 0 {
			avgPx = resOrder.CummulativeQuoteQty / resOrder.ExecutedQty
		}

		orders[index] = gateway.Order{
			Market:           market,
			ID:               resOrder.OrderID,
			State:            state,
			Amount:           resOrder.OrigQty,
			Price:            resOrder.Price,
			FilledAmount:     resOrder.ExecutedQty,
			FilledMoneyValue: resOrder.CummulativeQuoteQty,
			AvgPrice:         avgPx,
		}
	}

	return orders, nil
}

var insuficientBalanceMatch = regexp.MustCompile(`Insufficient account balance`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)

	params["symbol"] = order.Market.Symbol

	if order.Side == gateway.Bid {
		params["side"] = "BUY"
	} else {
		params["side"] = "SELL"
	}

	if order.Type == gateway.MarketOrder {
		params["type"] = "MARKET"
	} else {
		params["type"] = "LIMIT"
	}

	params["timeInForce"] = "GTC"
	params["quantity"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
	params["price"] = utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)

	res, err := g.api.NewOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return strconv.FormatInt(res.OrderID, 10), nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	params := make(map[string]string)

	params["symbol"] = order.Market.Symbol
	params["orderId"] = order.ID

	return g.api.CancelOrder(params)
}

func wsOrderToState(order WsOrderUpdate) gateway.OrderState {
	// Order event statuses
	// 1 - created by user
	// 2 - cancelled by user
	// 3 - filled by engine
	// 4 - cancelled by engine
	// Order statuses
	// 0 - The order has not been accepted by the engine.
	// 1 - The order has been accepted by the engine.
	// 2 - The order has been completed.
	// 3 - A part of the order has been filled.
	// 4 - The order has been canceled by the user.
	switch order.OrderStatus {
	case 0:
		return gateway.OrderCancelled
	case 1:
		return gateway.OrderOpen
	case 2:
		return gateway.OrderFullyFilled
	case 3:
		return gateway.OrderPartiallyFilled
	case 4:
		return gateway.OrderCancelled
	default:
		return gateway.OrderUnknown
	}
}
