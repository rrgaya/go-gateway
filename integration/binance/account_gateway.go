package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api    *binance.Client
	tickCh chan gateway.Tick
}

func NewAccountGateway(api *binance.Client, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:    api,
		tickCh: tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	// Start user data stream, to receive account updates
	listenKey, err := g.api.NewStartUserStreamService().Do(context.Background())
	if err != nil {
		return err
	}

	doneC, _, err := binance.WsUserDataServe(listenKey, g.userStreamHandler, g.userStreamErrHandler)
	if err != nil {
		return err
	}

	// Keepalive the stream, send a ping every 30m
	go func() {
		for {
			select {
			case <-doneC: // Websocket closed, stop sending keepalives
				return
			default:
				time.Sleep(15 * time.Minute)

				g.requestKeepaliveUserStream(listenKey)
			}
		}
	}()

	return nil
}

type UserStreamGenericUpdate struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
}

type UserStreamOrderUpdate struct {
	EventType                 string  `json:"e"`
	EventTime                 int64   `json:"E"`
	Symbol                    string  `json:"s"`        // Symbol
	ClientOrderID             string  `json:"c"`        // Client order ID
	Side                      string  `json:"S"`        // Side
	Type                      string  `json:"o"`        // Order type
	TimeInForce               string  `json:"f"`        // Time in force
	Quantity                  float64 `json:"q,string"` // Order quantity
	Price                     float64 `json:"p,string"` // Order price
	StopPrice                 float64 `json:"P,string"` // Stop price
	IcebergQuantity           float64 `json:"F,string"` // Iceberg quantity
	OriginalClientOrderID     string  `json:"C"`        // Original client order ID; This is the ID of the order being canceled
	CurrentExecutionType      string  `json:"x"`        // Current execution type
	CurrentOrderStatus        string  `json:"X"`        // Current order status
	OrderRejectReason         string  `json:"r"`        // Order reject reason; will be an error code.
	OrderID                   int64   `json:"i"`        // Order ID
	LastExecutedQuantity      float64 `json:"l,string"` // Last executed quantity
	CumulativeFilledQuantity  float64 `json:"z,string"` // Cumulative filled quantity
	LastExecutedPrice         float64 `json:"L,string"` // Last executed price
	CommissionAmount          float64 `json:"n,string"` // Commission amount
	CommissionAsset           string  `json:"N"`        // Commission asset
	TransactionTime           int64   `json:"T"`        // Transaction time
	TradeID                   int64   `json:"t"`        // Trade ID
	Working                   bool    `json:"w"`        // Is the order working? Stops will have
	Maker                     bool    `json:"m"`        // Is this trade the maker side?
	CreationTime              int64   `json:"O"`        // Order creation time
	CumulativeFilledQuote     float64 `json:"Z,string"` // Cumulative quote asset transacted quantity
	LastQuoteExecutedQuantity float64 `json:"Y,string"` // Last quote asset transacted quantity (i.e. lastPrice * lastQty)

	// Ignore (not sure what this is)
	g int64 `json:"g"`
	I int64 `json:"I"`
	M bool  `json:"M"`
}

func (g *AccountGateway) userStreamHandler(message []byte) {
	var genericUpdate UserStreamGenericUpdate

	err := json.Unmarshal(message, &genericUpdate)
	if err != nil {
		log.Printf("Failed to unmarshal Binance user stream update message error %s", err)
		return
	}

	switch genericUpdate.EventType {
	case "executionReport":
		var orderUpdate UserStreamOrderUpdate
		err := json.Unmarshal(message, &orderUpdate)
		if err != nil {
			log.Printf("Failed to unmarshal Binance user stream order update error %s", err)
			return
		}

		g.processOrderUpdate(orderUpdate)
	}
}

func (g *AccountGateway) userStreamErrHandler(err error) {
	panic(fmt.Errorf("Binance user data stream error %v. Halting...", err))
}

func (g *AccountGateway) requestKeepaliveUserStream(listenKey string) {
	err := g.api.NewKeepaliveUserStreamService().ListenKey(listenKey).Do(context.Background())
	if err != nil {
		log.Printf("Failed to send keepalive request to binance user data stream, error %s", err)
		log.Printf("Retrying keepalive request in 1 minute...")
		time.Sleep(1 * time.Minute)
		g.requestKeepaliveUserStream(listenKey)
	}
}

func (g *AccountGateway) processOrderUpdate(update UserStreamOrderUpdate) {
	events := make([]gateway.Event, 0, 2)

	switch update.CurrentExecutionType {
	case "NEW":
		events = append(events, g.newOkEventFromUpdate(update))
	case "CANCELED":
		events = append(events, g.newCancelEventFromUpdate(update))
	case "EXPIRED":
		events = append(events, g.newCancelEventFromUpdate(update))
	case "TRADE":
		events = append(events, g.newFillEventFromUpdate(update))
	}

	avgPx := 0.0
	if update.CumulativeFilledQuantity > 0 {
		avgPx = update.CumulativeFilledQuote / update.CumulativeFilledQuantity
	}

	events = append(events, gateway.NewOrderUpdateEvent(gateway.Order{
		ID:               strconv.FormatInt(update.OrderID, 10),
		Market:           gateway.Market{Symbol: update.Symbol},
		State:            binanceOrderStatusToCommonStatus(update.CurrentOrderStatus),
		Price:            update.Price,
		Amount:           update.Quantity,
		AvgPrice:         avgPx,
		FilledAmount:     update.CumulativeFilledQuantity,
		FilledMoneyValue: update.CumulativeFilledQuote,
	}))

	g.tickCh <- gateway.TickWithEvents(events...)
}

func (g *AccountGateway) newOkEventFromUpdate(update UserStreamOrderUpdate) gateway.Event {
	return gateway.NewOkEvent(gateway.Ok{
		Timestamp:     time.Unix(0, update.EventTime),
		Symbol:        update.Symbol,
		OrderID:       strconv.FormatInt(update.OrderID, 10),
		ClientOrderID: update.ClientOrderID,
	})
}

func (g *AccountGateway) newCancelEventFromUpdate(update UserStreamOrderUpdate) gateway.Event {
	return gateway.NewCancelEvent(gateway.Cancel{
		Timestamp:     time.Unix(0, update.EventTime),
		Symbol:        update.Symbol,
		OrderID:       strconv.FormatInt(update.OrderID, 10),
		ClientOrderID: update.ClientOrderID,
	})
}

func (g *AccountGateway) newFillEventFromUpdate(update UserStreamOrderUpdate) gateway.Event {
	var fullyFilled bool
	if update.CurrentOrderStatus == "FILLED" {
		fullyFilled = true
	}

	var side gateway.Side
	if update.Side == "BUY" {
		side = gateway.Bid
	} else {
		side = gateway.Ask
	}

	return gateway.NewFillEvent(gateway.Fill{
		Timestamp:     time.Unix(0, update.EventTime),
		Symbol:        update.Symbol,
		OrderID:       strconv.FormatInt(update.OrderID, 10),
		ClientOrderID: update.ClientOrderID,
		Side:          side,
		Amount:        update.LastQuoteExecutedQuantity,
		Price:         update.LastExecutedPrice,
		FullyFilled:   fullyFilled,
	})
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.NewGetAccountService().Do(context.Background())
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, len(res.Balances))

	for index, balance := range res.Balances {
		free, err := strconv.ParseFloat(balance.Free, 64)
		if err != nil {
			return balances, fmt.Errorf("Failed to parse asset %s \"free\" value %s into float", balance.Asset, balance.Free)
		}
		locked, err := strconv.ParseFloat(balance.Locked, 64)
		if err != nil {
			return balances, fmt.Errorf("Failed to parse asset %s \"locked\" value %s into float", balance.Asset, balance.Locked)
		}

		balances[index] = gateway.Balance{
			Asset:     balance.Asset,
			Available: free,
			Total:     free + locked,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	res, err := g.api.NewListOpenOrdersService().Symbol(market.Symbol).Do(context.Background())
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))

	for index, resOrder := range res {
		state := binanceOrderStatusToCommonStatus(string(resOrder.Status))

		amount, err := strconv.ParseFloat(resOrder.OrigQuantity, 64)
		if err != nil {
			return orders, fmt.Errorf("failed to parse resOrder.OrigQuantity %s to float: %s", resOrder.OrigQuantity, err)
		}
		price, err := strconv.ParseFloat(resOrder.Price, 64)
		if err != nil {
			return orders, fmt.Errorf("failed to parse resOrder.Price %s to float: %s", resOrder.Price, err)
		}
		filledAmount, err := strconv.ParseFloat(resOrder.ExecutedQuantity, 64)
		if err != nil {
			return orders, fmt.Errorf("failed to parse resOrder.ExecutedQuantity %s to float: %s", resOrder.ExecutedQuantity, err)
		}

		orders[index] = gateway.Order{
			Market:       market,
			ID:           strconv.FormatInt(int64(resOrder.OrderID), 10),
			State:        state,
			Amount:       amount,
			Price:        price,
			FilledAmount: filledAmount,
		}
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	var orderSide binance.SideType

	if order.Side == gateway.Bid {
		orderSide = binance.SideTypeBuy
	} else {
		orderSide = binance.SideTypeSell
	}

	orderType := binance.OrderTypeLimit
	if order.Type == gateway.MarketOrder {
		orderType = binance.OrderTypeMarket
	}

	res, err := g.api.NewCreateOrderService().
		Symbol(order.Market.Symbol).
		Side(orderSide).
		Type(orderType).
		TimeInForce(binance.TimeInForceTypeGTC).
		Quantity(utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)).
		Price(utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)).
		Do(context.Background())
	if err != nil {
		return "", err
	}

	return strconv.FormatInt(res.OrderID, 10), nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	orderId, err := strconv.ParseInt(order.ID, 10, 64)
	if err != nil {
		return err
	}

	_, err = g.api.NewCancelOrderService().
		Symbol(order.Market.Symbol).
		OrderID(orderId).
		Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func binanceOrderStatusToCommonStatus(status string) gateway.OrderState {
	switch status {
	case "NEW":
		return gateway.OrderOpen
	case "PARTIALLY_FILLED":
		return gateway.OrderPartiallyFilled
	case "FILLED":
		return gateway.OrderFullyFilled
	case "CANCELED":
		return gateway.OrderCancelled
	case "PENDING_CANCEL":
		return gateway.OrderOpen
	case "REJECTED":
		return gateway.OrderCancelled
	case "EXPIRED":
		return gateway.OrderCancelled
	}

	return ""
}
