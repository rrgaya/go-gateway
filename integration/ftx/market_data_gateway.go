package ftx

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

func (mg *MarketDataGateway) Connect(ws *WsSession) error {
	mg.ws = ws

	return nil
}

func (mg *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	mg.marketsMap = make(map[string]gateway.Market)

	for _, market := range markets {
		mg.marketsMap[market.Symbol] = market
	}

	log.Printf("FTX subscribing to %d markets...", len(markets))

	ch := make(chan WsResponse)
	mg.ws.SubscribeMessages(ch, nil)
	go mg.subscriptionMessageHandler(ch)

	var requests []WsRequest

	for _, market := range mg.marketsMap {
		marketsReq := WsRequest{
			Channel: "orderbook",
			Market:  market.Symbol,
			Op:      "subscribe",
		}

		tradesReq := WsRequest{
			Channel: "trades",
			Market:  market.Symbol,
			Op:      "subscribe",
		}

		requests = append(requests, marketsReq, tradesReq)
	}

	err := mg.ws.RequestSubscriptions(requests)
	if err != nil {
		return err
	}

	return nil
}

func (mg *MarketDataGateway) subscriptionMessageHandler(ch chan WsResponse) {
	for msg := range ch {
		if msg.Type == "subscribed" {
			continue
		}

		switch msg.Channel {
		case "orderbook":
			if err := mg.processOrderBook(msg); err != nil {
				log.Printf("FTX error processing \"%s\": %s", msg.Channel, err)
			}
		case "trades":
			if err := mg.processTrades(msg); err != nil {
				log.Printf("FTX error processing \"%s\": %s", msg.Channel, err)
			}
		}
	}
}

func (mg *MarketDataGateway) processOrderBook(msg WsResponse) error {
	var eventLogs []gateway.Event

	// Snapshot
	if msg.Type == "partial" {
		eventLogs = append(eventLogs, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: msg.Market,
			},
		})
	}

	appendEvents := func(symbol string, side gateway.Side, prices []gateway.PriceArray) {
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

			eventLogs = append(eventLogs, event)
		}
	}

	orderBook := WsOrderBook{}

	if err := json.Unmarshal(msg.Data, &orderBook); err != nil {
		return err
	}

	appendEvents(msg.Market, gateway.Bid, orderBook.Bids)
	appendEvents(msg.Market, gateway.Ask, orderBook.Asks)

	mg.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func (mg *MarketDataGateway) processTrades(msg WsResponse) error {
	tradeSideSelect := func(side string) gateway.Side {
		if side == "sell" {
			return gateway.Ask
		} else {
			return gateway.Bid
		}
	}

	trades := []WsTrade{}

	if err := json.Unmarshal(msg.Data, &trades); err != nil {
		return err
	}

	eventLogs := make([]gateway.Event, len(trades))

	for i, trade := range trades {
		eventLogs[i] = gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Timestamp: trade.Time,
				Symbol:    msg.Market,
				ID:        fmt.Sprintf("%v", trade.ID),
				Direction: tradeSideSelect(trade.Side),
				Amount:    trade.Size,
				Price:     trade.Price,
			},
		}
	}

	mg.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}
