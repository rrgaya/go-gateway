package bitso

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options    gateway.Options
	marketsMap map[string]gateway.Market
	tickCh     chan gateway.Tick
	ws         *WsSession
	api        *API
}

func NewMarketDataGateway(opts gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options:    opts,
		marketsMap: make(map[string]gateway.Market),
		tickCh:     tickCh,
	}
}

func (mktGtw *MarketDataGateway) Connect(ws *WsSession, api *API) error {
	mktGtw.ws = ws
	mktGtw.api = api

	return nil
}

func (mktGtw *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	mktGtw.marketsMap = make(map[string]gateway.Market)

	for _, market := range markets {
		mktGtw.marketsMap[market.Symbol] = market
	}

	log.Printf("Bitso subscribing to %d markets...", len(markets))

	ch := make(chan WsResponse)
	mktGtw.ws.SubscribeMessages(ch, nil)

	go mktGtw.subscriptionMessageHandler(ch)

	var requests []WsRequest

	for _, market := range mktGtw.marketsMap {
		// TODO: We dont ned this for now, we are not
		// subscring to the raw orderbook, once we are
		// this can return.
		//mktGtw.fetchOrderBookSnapshot(market.Symbol)
		//
		//orderBookRequest := WsRequest{
		//	Action: "subscribe",
		//	Symbol: market.Symbol,
		//	Type:   "diff-orders",

		orderBookRequest := WsRequest{
			Action: "subscribe",
			Symbol: market.Symbol,
			Type:   "orders",
		}

		tradesRequest := WsRequest{
			Action: "subscribe",
			Symbol: market.Symbol,
			Type:   "trades",
		}

		requests = append(requests, orderBookRequest, tradesRequest)
	}

	err := mktGtw.ws.RequestSubscriptions(requests)
	if err != nil {
		return err
	}

	return nil
}

func (mktGtw *MarketDataGateway) fetchOrderBookSnapshot(symbol string) error {
	var eventLogs []gateway.Event

	appendOrderEvents := func(symbol string, side gateway.Side, orders []ApiOrderBookResult) {
		for _, order := range orders {
			event := gateway.Event{
				Type: gateway.DepthEvent,
				Data: gateway.Depth{
					Symbol: symbol,
					Side:   side,
					Price:  order.Price,
					Amount: order.Amount,
				},
			}

			eventLogs = append(eventLogs, event)
		}
	}

	eventLogs = append(eventLogs, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: symbol,
		},
	})

	snapshot, err := mktGtw.api.FetchOrderBook(symbol)
	if err != nil {
		log.Printf("Bitso failed to fetch ob snapshot for symbol %s, err: %s", symbol, err)
		return err
	}

	appendOrderEvents(symbol, gateway.Bid, snapshot.Asks)
	appendOrderEvents(symbol, gateway.Ask, snapshot.Bids)

	mktGtw.dispatchEventLogs(eventLogs)

	return nil
}

func (mktGtw *MarketDataGateway) subscriptionMessageHandler(ch chan WsResponse) {
	printError := func(err error, channel string) {
		if err != nil {
			log.Printf("Bitso error processing msg from channel %q, err: %s\n", channel, err)
		}
	}

	for msg := range ch {
		switch msg.Type {
		case "orders":
			err := mktGtw.processOrderBookUpdate(msg)
			printError(err, msg.Type)
		case "diff-orders":
			err := mktGtw.processRawOrderBookUpdate(msg)
			printError(err, msg.Type)
		case "trades":
			err := mktGtw.processTrades(msg)
			printError(err, msg.Type)
		}
	}
}

func (mktGtw *MarketDataGateway) processOrderBookUpdate(msg WsResponse) error {
	var eventLogs []gateway.Event

	var book WsOrders

	// As far as I checked, we receive an empty payload as the first
	// message sent for this method.
	if len(msg.Payload) <= 0 {
		return nil
	}

	if err := json.Unmarshal(msg.Payload, &book); err != nil {
		log.Printf("Failed %s, msg: %s", err, string(msg.Payload))
		return err
	}

	eventLogs = append(eventLogs, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: msg.Symbol,
		},
	})

	depths := make(map[string]gateway.Depth)
	for _, order := range append(book.Bids, book.Asks...) {
		price, err := order.Rate.Float64()
		if err != nil {
			log.Printf("Bitso failed to parse rate %v into float64", order.Rate)
			continue
		}

		amount, err := order.Amount.Float64()
		if err != nil {
			log.Printf("Bitso failed to parse amount %v into float64", order.Amount)
			continue
		}

		priceStr := order.Rate.String()

		depth, ok := depths[priceStr]
		if !ok {
			depth = gateway.Depth{
				Symbol: msg.Symbol,
				Side:   normalizeSide(order.Side),
				Price:  price,
				Amount: amount,
			}
		} else {
			depth.Amount += amount
		}

		depths[priceStr] = depth
	}

	for _, depth := range depths {
		event := gateway.Event{
			Type: gateway.DepthEvent,
			Data: depth,
		}

		eventLogs = append(eventLogs, event)
	}

	mktGtw.dispatchEventLogs(eventLogs)

	return nil
}

func (mktGtw *MarketDataGateway) processRawOrderBookUpdate(msg WsResponse) error {
	var eventLogs []gateway.Event

	orders := []WsOrder{}

	// As far as I checked, we receive an empty payload as the first
	// message sent for this method.
	if len(msg.Payload) <= 0 {
		return nil
	}

	if err := json.Unmarshal(msg.Payload, &orders); err != nil {
		return err
	}

	for _, order := range orders {
		price, err := order.Rate.Float64()
		if err != nil {
			log.Printf("Bitso failed to parse rate %v into float64", order.Rate)
			continue
		}

		amount, err := order.Amount.Float64()
		if err != nil {
			log.Printf("Bitso failed to parse amount %v into float64", order.Amount)
			continue
		}

		event := gateway.Event{
			Type: gateway.RawOrderEvent,
			Data: gateway.RawOrder{
				ID:     order.OrderID,
				Symbol: msg.Symbol,
				Side:   normalizeSide(order.Side),
				Price:  price,
				Amount: amount,
			},
		}

		eventLogs = append(eventLogs, event)
	}

	mktGtw.dispatchEventLogs(eventLogs)

	return nil
}

func (mktGtw *MarketDataGateway) processTrades(msg WsResponse) error {
	var eventLogs []gateway.Event

	trades := []WsTrade{}

	// As far as I checked, we receive an empty payload as the first
	// message sent for this method.
	if len(msg.Payload) <= 0 {
		return nil
	}

	if err := json.Unmarshal(msg.Payload, &trades); err != nil {
		return err
	}

	for _, trade := range trades {
		makerSide := tradeNormalizeSide(trade.MakerSide)
		var takerSide gateway.Side
		if makerSide == gateway.Bid {
			takerSide = gateway.Ask
		} else {
			takerSide = gateway.Bid
		}

		eventLogs = append(eventLogs, gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Symbol:    msg.Symbol,
				ID:        fmt.Sprintf("%s_%s", trade.TackerOrderID, trade.MarketOrderID),
				Direction: takerSide,
				Amount:    trade.Amount,
				Price:     trade.Rate,
			},
		})
	}

	mktGtw.dispatchEventLogs(eventLogs)

	return nil
}

func (mktGtw *MarketDataGateway) dispatchEventLogs(events []gateway.Event) {
	mktGtw.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func normalizeSide(t int8) gateway.Side {
	if t == 1 {
		return gateway.Bid
	}

	return gateway.Ask
}

func tradeNormalizeSide(selling int8) gateway.Side {
	if selling == 0 {
		return gateway.Bid
	}

	return gateway.Ask
}
