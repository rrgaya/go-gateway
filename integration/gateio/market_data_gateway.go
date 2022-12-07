package gateio

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
}

func NewMarketDataGateway(opts gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options:    opts,
		marketsMap: make(map[string]gateway.Market),
		tickCh:     tickCh,
	}
}

func (mktGtw *MarketDataGateway) Connect(ws *WsSession) error {
	mktGtw.ws = ws
	return nil
}

func (mktGtw *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	mktGtw.marketsMap = make(map[string]gateway.Market)

	for _, market := range markets {
		mktGtw.marketsMap[market.Symbol] = market
	}

	log.Printf("Gate.io subscribing to %d markets...", len(markets))

	ch := make(chan WsResponse)
	mktGtw.ws.SubscribeMessages(ch, nil)
	go mktGtw.subscriptionMessageHandler(ch)

	var requests []WsRequest

	for _, market := range mktGtw.marketsMap {
		// https://www.gate.io/docs/developers/apiv4/ws/en/#changed-order-book-levels

		bookSnapReq := WsRequest{
			Time:    time.Now().UnixMilli(),
			Channel: "spot.order_book",
			Event:   "subscribe",
			Payload: [3]string{market.Symbol, "20", "1000ms"},
		}

		orderBookReq := WsRequest{
			Time:    time.Now().UnixMilli(),
			Channel: "spot.order_book_update",
			Event:   "subscribe",
			Payload: [2]string{market.Symbol, "100ms"},
		}

		traderReq := WsRequest{
			Time:    time.Now().UnixMilli(),
			Channel: "spot.trades",
			Event:   "subscribe",
			Payload: [1]string{market.Symbol},
		}

		requests = append(requests, bookSnapReq, orderBookReq, traderReq)
	}

	err := mktGtw.ws.RequestSubscriptions(requests)
	if err != nil {
		return err
	}

	return nil
}

func (mktGtw *MarketDataGateway) subscriptionMessageHandler(ch chan WsResponse) {
	for msg := range ch {
		switch msg.Channel {
		case "spot.order_book":
			if err := mktGtw.processOrderBookSnapshot(msg); err != nil {
				log.Printf("Gate.io error processing \"%s\": %s", msg.Channel, err)
			}
		case "spot.order_book_update":
			if err := mktGtw.processOrderBook(msg); err != nil {
				log.Printf("Gate.io error processing \"%s\": %s", msg.Channel, err)
			}
		case "spot.trades":
			if err := mktGtw.processTrades(msg); err != nil {
				log.Printf("Gate.io error processing \"%s\": %s", msg.Channel, err)
			}
		}
	}
}

func appendOrderEvents(symbol string, side gateway.Side, prices []gateway.PriceArray, eventLogs *[]gateway.Event) {
	for _, order := range prices {
		event := gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   side,
				Price:  order.Price,
				Amount: order.Amount,
			},
		}

		*eventLogs = append(*eventLogs, event)
	}
}

func (mktGtw *MarketDataGateway) processOrderBookSnapshot(msg WsResponse) error {
	var eventLogs []gateway.Event

	result := WsOrderBookSnapshot{}

	if err := json.Unmarshal(msg.Result, &result); err != nil {
		return err
	}

	eventLogs = append(eventLogs, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: result.Pair,
		},
	})

	appendOrderEvents(result.Pair, gateway.Bid, result.Bids, &eventLogs)
	appendOrderEvents(result.Pair, gateway.Ask, result.Asks, &eventLogs)

	mktGtw.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func (mktGtw *MarketDataGateway) processOrderBook(msg WsResponse) error {
	var eventLogs []gateway.Event

	result := WsOrderBookUpdateResult{}

	if err := json.Unmarshal(msg.Result, &result); err != nil {
		return err
	}

	appendOrderEvents(result.Pair, gateway.Bid, result.Bids, &eventLogs)
	appendOrderEvents(result.Pair, gateway.Ask, result.Asks, &eventLogs)

	mktGtw.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func (mktGtw *MarketDataGateway) processTrades(msg WsResponse) error {
	var eventLogs []gateway.Event

	tradeSideSelect := func(side string) gateway.Side {
		if side == "sell" {
			return gateway.Ask
		} else {
			return gateway.Bid
		}
	}

	tradeResult := WsTradeResult{}

	if err := json.Unmarshal(msg.Result, &tradeResult); err != nil {
		return err
	}

	event := gateway.Event{
		Type: gateway.TradeEvent,
		Data: gateway.Trade{
			Timestamp: time.Unix(0, tradeResult.CreatedTime*int64(time.Millisecond)),
			Symbol:    tradeResult.CurrencyPair,
			ID:        fmt.Sprintf("%v", tradeResult.ID),
			Direction: tradeSideSelect(tradeResult.Side),
			Amount:    tradeResult.Amount,
			Price:     tradeResult.Price,
		},
	}

	eventLogs = append(eventLogs, event)

	mktGtw.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}
