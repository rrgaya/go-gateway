package bigone

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

var insuficientBalanceMatch = regexp.MustCompile(`10014`)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     APIClient
	options gateway.Options
	tickCh  chan gateway.Tick
}

func NewAccountGateway(api APIClient, options gateway.Options, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		api:     api,
		options: options,
		tickCh:  tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	return nil
}

func (g *AccountGateway) Balances() (balances []gateway.Balance, err error) {
	res, err := g.api.SpotAccounts()
	if err != nil {
		return balances, err
	}

	balances = make([]gateway.Balance, 0)
	for _, balance := range res {
		balances = append(balances, gateway.Balance{
			Asset:     balance.AssetSymbol,
			Total:     balance.Balance,
			Available: balance.Balance - balance.LockedBalance,
		})
	}

	return balances, nil
}

func (g *AccountGateway) subscribeMarketsUpdate(markets []gateway.Market) error {
	batchesOf := 100
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	for index, market := range markets {
		group := index / batchesOf

		if batches[group] == nil {
			batches[group] = make([]gateway.Market, 0)
		}

		batches[group] = append(batches[group], market)
	}

	log.Printf("BigONE has [%d] markets, will need to distribute account data subscriptions in %d websocket connections, maximum of [%d] subscriptions on each websocket.", len(markets), len(batches), batchesOf)

	for _, batch := range batches {
		err := g.connectWs(batch, nil, 3, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *AccountGateway) connectWs(markets []gateway.Market, proxy *url.URL, maxRetries, retryCount int) error {
	onDisconnect := make(chan error)
	ws := NewWsSession(g.options, onDisconnect)

	if proxy != nil {
		ws.SetProxy(proxy)
	}

	retryCount = 0
	go func() {
		err := <-onDisconnect
		if g.options.Verbose {
			log.Printf("BigONE account data gateway ws disconnected, err: %s", err)
			log.Printf("BigONE reconnecting in 5 seconds...")
		}
		time.Sleep(3 * time.Second)
		err = g.connectWs(markets, proxy, maxRetries, retryCount)
		if err != nil {
			err = fmt.Errorf("BigONE failed to reconnect after disconnect, err: %s", err)
			panic(err)
		}
	}()

	err := g.connectWsAndSubscribeAccountData(ws, markets)
	if err != nil {
		log.Printf("BigONE ws [%s] [%s] failed to connect and subscribe to market data [retry count %d], err: %s", proxy, markets, retryCount, err)
		if retryCount > maxRetries {
			err = fmt.Errorf("Reached maximum connect retry count of %d, err: %s", maxRetries, err)
			return err
		}
		time.Sleep(3 * time.Second)
		return g.connectWs(markets, proxy, maxRetries, retryCount+1)
	}

	return nil
}

func (g *AccountGateway) connectWsAndSubscribeAccountData(ws *WsSession, markets []gateway.Market) error {
	err := ws.Connect()
	if err != nil {
		return err
	}

	err = ws.Authenticate()
	if err != nil {
		err = fmt.Errorf("failed to auth ws, err: %s", err)
		return err
	}

	err = g.subscribeOrders(ws, markets)
	if err != nil {
		return err
	}

	return nil
}

func (g *AccountGateway) subscribeOrders(ws *WsSession, markets []gateway.Market) error {
	resChs := make(map[gateway.Market]chan []byte)
	for _, mkt := range markets {
		subReq := WsRequest{
			SubscribeViewerOrdersRequest: &WsMarketRequest{
				Market: mkt.Symbol,
			},
		}

		resCh, err := ws.SendRequestAsync(&subReq)
		if err != nil {
			return err
		}

		resChs[mkt] = resCh
	}

	for market, resCh := range resChs {
		go func(market gateway.Market, resCh chan []byte) {
			for msg := range resCh {
				_, dataType, _, _ := jsonparser.Get(msg, "error")
				if dataType != jsonparser.NotExist {
					log.Printf("Account order update mkt [%s] returned err [%s]", market, string(msg))
					continue
				}

				err := g.processOrderUpdate(msg)
				if err != nil {
					log.Printf("Failed to process order update data msg [%s] for market [%s], err [%s]", msg, market, err)
				}
			}
		}(market, resCh)
	}

	return nil
}

func (g *AccountGateway) processOrderUpdate(msg []byte) error {
	_, dataType, _, _ := jsonparser.Get(msg, "orderSnapshot")
	if dataType != jsonparser.NotExist {
		// Ignore probably a snapshot
		return nil
	}

	dataVal, dataType, _, _ := jsonparser.Get(msg, "orderUpdate", "order")
	if dataType == jsonparser.NotExist {
		log.Printf("Received msg on order update ch but didn't contain order update, msg: %s", string(msg))
		return nil
	}

	var order WsOrder
	err := json.Unmarshal(dataVal, &order)
	if err != nil {
		return fmt.Errorf("Failed to unmarshal order update [%s] err [%s]", string(msg), err)
	}

	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(gateway.Order{
		ID:               order.ID,
		Market:           gateway.Market{Symbol: order.Market},
		Side:             mapWsOrderSide(order.Side),
		State:            mapWsOrderStatusToState(order.State),
		Price:            order.Price,
		Amount:           order.Amount,
		AvgPrice:         order.AvgDealPrice,
		FilledAmount:     order.FilledAmount,
		FilledMoneyValue: order.FilledAmount * order.AvgDealPrice,
	}))

	return nil
}

func mapWsOrderSide(side string) gateway.Side {
	if side == "BID" {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func mapWsOrderStatusToState(status string) gateway.OrderState {
	switch status {
	case "PENDING":
		return gateway.OrderOpen
	case "FIRED":
		return gateway.OrderPartiallyFilled
	case "FILLED":
		return gateway.OrderFullyFilled
	case "CANCELLED":
		return gateway.OrderCancelled
	case "REJECTED":
		return gateway.OrderCancelled
	}
	return gateway.OrderUnknown
}

func (g *AccountGateway) OpenOrders(market gateway.Market) (orders []gateway.Order, err error) {
	params := url.Values{}
	params.Set("asset_pair_name", market.Symbol)
	params.Set("state", "PENDING")

	apiOrders, err := g.api.ListOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders = make([]gateway.Order, len(apiOrders))
	for i, record := range apiOrders {
		orders[i] = mapAPIOrderToGtw(market, record)
	}

	return orders, nil
}

func mapOrderToAPISide(order gateway.Order) string {
	if order.Side == gateway.Bid {
		return "BID"
	} else {
		return "ASK"
	}
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	req := APICreateOrder{
		AssetPairName: order.Market.Symbol,
		Price:         utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
		Amount:        utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		PostOnly:      order.PostOnly,
		Type:          "LIMIT",
		Side:          mapOrderToAPISide(order),
	}

	res, err := g.api.CreateOrder(req)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return strconv.FormatInt(res.ID, 10), err
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	_, err := g.api.CancelOrder(order.ID)
	if err != nil {
		return err
	}

	return nil
}

func mapAPIOrderToGtw(market gateway.Market, order APIOrder) gateway.Order {
	return gateway.Order{
		ID:               strconv.FormatInt(order.ID, 10),
		Market:           market,
		Side:             mapWsOrderSide(order.Side),
		State:            mapWsOrderStatusToState(order.State),
		Price:            order.Price,
		Amount:           order.Amount,
		AvgPrice:         order.AvgDealPrice,
		FilledAmount:     order.FilledAmount,
		FilledMoneyValue: order.FilledAmount * order.AvgDealPrice,
	}
}
