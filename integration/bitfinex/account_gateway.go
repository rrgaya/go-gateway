package bitfinex

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"sync"
	"time"

	bfxorder "github.com/bitfinexcom/bitfinex-api-go/pkg/models/order"
	bfxexec "github.com/bitfinexcom/bitfinex-api-go/pkg/models/tradeexecution"
	"github.com/bitfinexcom/bitfinex-api-go/v2/rest"
	"github.com/bitfinexcom/bitfinex-api-go/v2/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

const BFX_MAX_CID_VALUE = ((1 << 45) / 2) - 1 // uInt45

var insuficientBalanceMatch = regexp.MustCompile(`not enough exchange balance`)

type AccountGateway struct {
	gateway.BaseAccountGateway
	rest              *rest.Client
	ws                *websocket.Client
	tickCh            chan gateway.Tick
	options           gateway.Options
	orders            map[int64]gateway.Order
	ordersLock        *sync.Mutex
	ordersCb          map[int64]chan gateway.Order
	ordersCbLock      *sync.Mutex
	orderIDToCID      map[int64]int64
	orderCancelCb     map[int64]chan error
	orderCancelCbLock *sync.Mutex
}

func NewAccountGateway(options gateway.Options, ws *websocket.Client, rest *rest.Client, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options:           options,
		tickCh:            make(chan gateway.Tick),
		orders:            make(map[int64]gateway.Order),
		ordersLock:        &sync.Mutex{},
		ordersCb:          make(map[int64]chan gateway.Order),
		ordersCbLock:      &sync.Mutex{},
		orderIDToCID:      make(map[int64]int64),
		orderCancelCb:     make(map[int64]chan error),
		orderCancelCbLock: &sync.Mutex{},
		rest:              rest,
		ws:                ws,
	}
}

func (g *AccountGateway) Connect(authEvent chan websocket.AuthEvent) error {
	select {
	case event := <-authEvent:
		if event.Status == "OK" {
			return nil
		} else {
			return fmt.Errorf("Bitfinex failed auth, status: %s, message: %s", event.Status, event.Message)
		}
	case <-time.After(15 * time.Second):
		return fmt.Errorf("timed out waiting for ws auth")
	}
}

func (g *AccountGateway) processOrderRequest(id, cid int64, status, err string) {
	g.ordersLock.Lock()
	g.ordersCbLock.Lock()
	defer g.ordersLock.Unlock()
	defer g.ordersCbLock.Unlock()

	cb, ok := g.ordersCb[cid]
	if !ok {
		//log.Printf("Bfx failed to find orderCb cid [%d] on orders map on processOrderOpenError", cid)
		return
	}

	order, ok := g.orders[cid]
	if !ok {
		//log.Printf("Bfx failed to find order cid [%d] on orders map on processOrderOpenError", cid)
		return
	}

	if status != "SUCCESS" {
		order.Error = fmt.Errorf("%s", err)
		delete(g.orders, cid)
	} else {
		order.ID = strconv.FormatInt(id, 10)
		order.State = gateway.OrderOpen
		g.orders[cid] = order
		g.orderIDToCID[id] = cid
	}

	cb <- order
}

func (g *AccountGateway) processOrderCancel(id int64, status, text string) {
	g.orderCancelCbLock.Lock()
	defer g.orderCancelCbLock.Unlock()

	cb, ok := g.orderCancelCb[id]
	if !ok {
		//log.Printf("Bfx failed to find order id [%d] on orders map on processOrderCancel", id)
		return
	}

	if status != "SUCCESS" {
		err := fmt.Errorf("order cancel fail, status: %s, reason: %s", status, text)
		cb <- err
	} else {
		cb <- nil
	}
}

func (g *AccountGateway) processOrderUpdate(id, cid int64, status string) {
	g.ordersLock.Lock()
	defer g.ordersLock.Unlock()

	order, ok := g.orders[cid]
	if !ok {
		//log.Printf("Bfx failed to find order cid [%d] on orders map on processOrderUpdate", cid)
		return
	}

	order.State = mapStatusToCommon(status)
	g.orders[cid] = order

	if status == "CANCELLED" {
		cancelCb := g.orderCancelCb[id]
		if cancelCb != nil {
			cancelCb <- nil
		}
	}

	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) processTradeExecution(exec *bfxexec.TradeExecution) {
	g.ordersLock.Lock()
	defer g.ordersLock.Unlock()

	cid, ok := g.orderIDToCID[exec.OrderID]
	if !ok {
		//log.Printf("Bfx failed to find map trade exec.OrderID [%d] to CID", exec.OrderID)
		return
	}

	order, ok := g.orders[cid]
	if !ok {
		//log.Printf("Bfx failed to find order cid [%d] on orders map on processTradeExecution", cid)
		return
	}

	order.FilledAmount = math.Abs(exec.ExecAmount)
	g.orders[cid] = order
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.rest.Wallet.Wallet()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, len(res.Snapshot))
	for i, wallet := range res.Snapshot {
		balances[i] = gateway.Balance{
			Asset:     wallet.Currency,
			Available: wallet.BalanceAvailable,
			Total:     wallet.Balance,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	res, err := g.rest.Orders.GetBySymbol(market.Symbol)
	if err != nil {
		// TODO: bug on the get by symbols, when no open orders
		// it is erroring
		log.Printf("Error on fetch bfx open orders, err: %s", err)
		return orders, nil
	}

	orders = make([]gateway.Order, len(res.Snapshot))
	for i, order := range res.Snapshot {
		orders[i] = mapOrderToCommon(market, *order)
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	var amount = order.Amount
	if order.Side == gateway.Ask {
		amount = amount * -1
	}

	cid := rand.Int63n(BFX_MAX_CID_VALUE)
	cb := make(chan gateway.Order, 1)

	g.ordersLock.Lock()
	g.ordersCbLock.Lock()

	order.State = gateway.OrderSent
	g.orders[cid] = order
	g.ordersCb[cid] = cb

	g.ordersLock.Unlock()
	g.ordersCbLock.Unlock()

	req := &bfxorder.NewRequest{
		CID:      cid,
		Symbol:   order.Market.Symbol,
		Type:     "EXCHANGE LIMIT",
		Price:    order.Price,
		Amount:   amount,
		PostOnly: order.PostOnly,
	}
	ctx, cxl1 := context.WithTimeout(context.Background(), time.Second*5)
	defer cxl1()
	err = g.ws.SubmitOrder(ctx, req)
	if err != nil {
		return "", err
	}

	select {
	case order := <-cb:
		if order.Error != nil {
			if insuficientBalanceMatch.MatchString(order.Error.Error()) {
				return order.ID, gateway.InsufficientBalanceErr
			} else {
				return order.ID, order.Error
			}
		} else {
			return order.ID, nil
		}
	case <-time.After(10 * time.Second):
		return "", fmt.Errorf("timed out after waiting or 10 seconds for order to open")
	}
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	orderId, err := strconv.ParseInt(order.ID, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse int: %s", err)
	}

	req := &bfxorder.CancelRequest{
		ID: orderId,
	}

	cb := make(chan error, 1)
	g.orderCancelCbLock.Lock()
	g.orderCancelCb[orderId] = cb
	g.orderCancelCbLock.Unlock()

	ctx, cxl1 := context.WithTimeout(context.Background(), time.Second*5)
	defer cxl1()
	err = g.ws.SubmitCancel(ctx, req)
	if err != nil {
		return err
	}

	select {
	case err := <-cb:
		return err
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timed out after waiting for 10 seconds for order to cancel")
	}
}

func mapOrderToCommon(mkt gateway.Market, order bfxorder.Order) gateway.Order {
	var side gateway.Side
	if order.Amount > 0 {
		side = gateway.Bid
	} else {
		side = gateway.Ask
	}

	return gateway.Order{
		ID:       strconv.FormatInt(order.ID, 10),
		Market:   mkt,
		State:    mapStatusToCommon(order.Status),
		Side:     side,
		Price:    order.Price,
		Amount:   math.Abs(order.Amount),
		AvgPrice: order.PriceAvg,
	}
}

func mapStatusToCommon(status string) gateway.OrderState {
	switch status {
	case "ACTIVE":
		return gateway.OrderOpen
	case "EXECUTED":
		return gateway.OrderFullyFilled
	case "PARTIALLY FILLED":
		return gateway.OrderPartiallyFilled
	case "CANCELED":
		return gateway.OrderCancelled
	}
	return gateway.OrderUnknown
}
