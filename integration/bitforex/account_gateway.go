package bitforex

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api             *Api
	webapi          *WebApi
	options         gateway.Options
	markets         []gateway.Market
	marketsMap      map[string]gateway.Market
	userId          int64
	openOrders      map[string]gateway.Order
	openOrdersMutex *sync.Mutex
	tickCh          chan gateway.Tick
}

func NewAccountGateway(api *Api, webapi *WebApi, options gateway.Options, markets []gateway.Market, tickCh chan gateway.Tick) *AccountGateway {
	marketsMap := make(map[string]gateway.Market)
	for _, market := range markets {
		marketsMap[market.Symbol] = market
	}

	return &AccountGateway{
		webapi:          webapi,
		api:             api,
		options:         options,
		markets:         markets,
		marketsMap:      marketsMap,
		openOrders:      make(map[string]gateway.Order),
		openOrdersMutex: &sync.Mutex{},
		tickCh:          tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	userInfo, err := g.webapi.GetUserInfo()
	if err != nil {
		return fmt.Errorf("failed to fetch user info, err: %s", err)
	}

	g.userId = userInfo.UserId
	log.Printf("Bitforex user id: %d", g.userId)

	/* TODO: Their websocket is not working yet, we cant receive order updates, we have to keep polling for now */
	messageCh := make(chan WsMessage)

	wsToken, err := g.webapi.GetWsToken()
	if err != nil {
		return fmt.Errorf("failed to get user ws token, err: %s", err)
	}
	log.Printf("Bitforex ws token: %s", wsToken)

	ws := NewWsSession(g.options, messageCh)
	err = ws.Connect(WsOrdersApi)
	if err != nil {
		return fmt.Errorf("failed to connect to ws orders api, err: %s", err)
	}

	// Auth
	err = g.wsAuth(ws, g.userId, wsToken, messageCh)
	if err != nil {
		return err
	}

	for _, market := range g.markets {
		// Subscribe to each market order updates
		err = g.subscribeOrderUpdate(ws, market)
		if err != nil {
			ws.Close()
			return fmt.Errorf("failed to subscribe to markets depth, err: %s", err)
		}
	}

	go g.messageHandler(messageCh)

	//go g.webapiConnectionChecker()
	//go g.trackOpenOrders()

	return nil
}

// This will keep the http connection "hot" (keep-alive)
// And will also check if we are still logged in
func (g *AccountGateway) webapiConnectionChecker() {
	for {
		requestAt := time.Now()
		userInfo, err := g.webapi.GetUserInfo()
		if err != nil {
			panic(fmt.Errorf("Failed to fetch user info, err: %s", err))
		}
		executedIn := time.Now().UnixNano() - requestAt.UnixNano()

		if g.options.Verbose {
			log.Printf("Received connection info (user id %d) response time %fms", userInfo.UserId, float64(executedIn)/float64(time.Millisecond))
		}

		time.Sleep(60 * time.Second)
	}
}

func (g *AccountGateway) marketsWithOpenOrders() []gateway.Market {
	g.openOrdersMutex.Lock()
	defer g.openOrdersMutex.Unlock()

	marketsMap := make(map[string]gateway.Market)
	for _, order := range g.openOrders {
		marketsMap[order.Market.Symbol] = order.Market
	}

	markets := make([]gateway.Market, 0, len(marketsMap))
	for _, market := range marketsMap {
		markets = append(markets, market)
	}

	return markets
}

func (g *AccountGateway) trackOpenOrders() {
	for {
		markets := g.marketsWithOpenOrders()

		for _, market := range markets {
			log.Printf("Updating open orders for market %s...", market)
			go g.updateOpenOrdersForMarket(market)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (g *AccountGateway) updateOpenOrdersForMarket(market gateway.Market) {
	openOrders, err := g.webapi.GetOrders(market.Symbol, webapiGetUncompleteOrders)
	if err != nil {
		log.Printf("Bitforex faied to fetch open orders for symbol %s, err: %s", market.Symbol, err)
		return
	}
	completeOrders, err := g.webapi.GetOrders(market.Symbol, webapiGetCompleteOrders)
	if err != nil {
		log.Printf("Bitforex faied to fetch complete orders for symbol %s, err: %s", market.Symbol, err)
		return
	}

	g.openOrdersMutex.Lock()

mainLoop:
	for _, trackedOrder := range g.openOrders {
		// If the order is still open, check for changes, and notify and update it's state if updated
		for _, openOrder := range openOrders {
			if openOrder.OrderId == trackedOrder.ID {
				order := mapWebOrderToCommon(market, openOrder)

				if trackedOrder.State != order.State || trackedOrder.FilledAmount != order.FilledAmount {
					go g.dispatchOrderUpdate(order)
					g.openOrders[openOrder.OrderId] = order
				}

				continue mainLoop
			}
		}

		// If the tracked order is in the completed orders list, send the last update, and stop tracking it
		for _, completeOrder := range completeOrders {
			if completeOrder.OrderId == trackedOrder.ID {
				order := mapWebOrderToCommon(market, completeOrder)

				go g.dispatchOrderUpdate(order)
				delete(g.openOrders, completeOrder.OrderId)

				continue mainLoop
			}
		}
	}

	g.openOrdersMutex.Unlock()
}

func (g *AccountGateway) dispatchOrderUpdate(order gateway.Order) {
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) wsAuth(ws *WsSession, uid int64, wsToken string, messageCh chan WsMessage) error {
	req, err := NewWsRequest("ck_auth", "auth", WsAuthParam{
		UID:   uid,
		Token: wsToken,
	})
	if err != nil {
		return fmt.Errorf("failed build auth req, err: %s", err)
	}
	err = ws.SendRequest(req)
	if err != nil {
		return fmt.Errorf("failed to send auth request, err: %s", err)
	}

	// Next frame should be the auht req response
	select {
	case msg := <-messageCh:
		if msg.Event != "auth" {
			return fmt.Errorf("expected first ws msg event after auth req to be \"auth\", was \"%s\" instead", msg.Event)
		}
		if msg.Success != true {
			return fmt.Errorf("ws auth req response was not successfull, received msg %+v", msg)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out while waiting for auth req response")
	}

	return nil
}

func (g *AccountGateway) subscribeOrderUpdate(ws *WsSession, market gateway.Market) error {
	request, err := g.buildOrderSubRequest(market)
	if err != nil {
		return fmt.Errorf("failed to build market order sub request for market %s, err: %s", market, err)
	}

	err = ws.SendRequest(request)
	if err != nil {
		return fmt.Errorf("failed to send order sub request for market %s, err: %s", market, err)
	}

	return nil
}

func (g *AccountGateway) buildOrderSubRequest(market gateway.Market) (WsRequest, error) {
	param := WsOrderSubParam{
		BusinessType: market.Symbol,
	}

	return NewWsRequest("sub_ord", "ord_change", param)
}

func (g *AccountGateway) messageHandler(wsMessage chan WsMessage) {
	for {
		message, ok := <-wsMessage
		if !ok {
			// Channel closed
			return
		}

		if message.Event == "ord_change" {
			symbol, err := jsonparser.GetString(message.Data, "businessType")
			if err != nil {
				log.Printf("Failed to parse ord_change event data, err: %s", err)
				continue
			}

			log.Printf("Received ord_change %s for %s: %+v", message.Type, symbol, message)

			switch message.Type {
			case "sub_ord": // Subscribe to order updates
				if message.Success != true {
					log.Printf("Bitforex failed to subscribe to %s order update. %+v", symbol, message)
				}
			case "ord_book": // Update open orders
			case "ord_all": // Update all orders
			}
		}
	}
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	funds, err := g.api.FundsInfo()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, len(funds))
	for i, fund := range funds {
		balances[i] = gateway.Balance{
			Asset:     strings.ToUpper(fund.Currency),
			Total:     fund.Fix,
			Available: fund.Active,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	return []gateway.Order{}, errors.New("not implemented")
}

func (g *AccountGateway) SendOrder(order gateway.Order) (string, error) {
	var side tradeType
	if order.Side == gateway.Bid {
		side = buyTradeType
	} else {
		side = sellTradeType
	}

	market := order.Market
	symbol := market.Symbol
	price := utils.FloatToStringWithTick(order.Price, market.PriceTick)
	amount := utils.FloatToStringWithTick(order.Amount, market.AmountTick)

	res, err := g.api.PlaceOrder(side, symbol, price, amount)
	if err != nil {
		return "", err
	}

	// Register open order
	order.ID = res
	g.openOrdersMutex.Lock()
	g.openOrders[order.ID] = order
	g.openOrdersMutex.Unlock()

	return order.ID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder(order.Market.Symbol, order.ID)
}

func mapWebOrderToCommon(market gateway.Market, order WebOrder) gateway.Order {
	return gateway.Order{
		Market:           market,
		ID:               order.OrderId,
		State:            mapWebOrderToCommonStatus(order),
		Side:             mapOrderSideToCommon(order.TradeType),
		Amount:           order.OrderAmount,
		Price:            order.OrderPrice,
		AvgPrice:         order.PriceAvg,
		FilledAmount:     order.FilledAmount,
		FilledMoneyValue: order.FilledAmount * order.PriceAvg,
	}
}

func mapWebOrderToCommonStatus(order WebOrder) gateway.OrderState {
	if order.OrderState == cancelledOrderState {
		return gateway.OrderCancelled
	} else if order.OrderState == fullyFilledOrderState {
		return gateway.OrderFullyFilled
	} else if order.FilledAmount > 0 && order.FilledAmount < order.OrderAmount {
		return gateway.OrderPartiallyFilled
	} else if order.OrderState == openOrderState {
		return gateway.OrderOpen
	}

	log.Printf("Bitforex unable to deduce order %+v state", order)
	return ""
}

func mapOrderSideToCommon(side int64) gateway.Side {
	if side == 2 {
		return gateway.Ask
	} else {
		return gateway.Bid
	}
}
