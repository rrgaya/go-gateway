package bitmart

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options    gateway.Options
	marketsMap map[string]gateway.Market
	tickCh     chan gateway.Tick
}

func NewMarketDataGateway(opts gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options:    opts,
		marketsMap: make(map[string]gateway.Market),
		tickCh:     tickCh,
	}
}

func (mg *MarketDataGateway) Connect() error {
	return nil
}

const MAX_BOOK_DEPTH = 50

func (mg *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	ws := NewWsSession(mg.options)
	err := ws.Connect(WsPublicURL)
	if err != nil {
		return fmt.Errorf("failed to connect to ws, err: %s", err)
	}

	for _, market := range markets {
		mg.marketsMap[market.Symbol] = market
	}

	log.Printf("%s subscribing to %d markets...", Exchange.Name, len(markets))

	ch := make(chan WsResponse)
	ws.SubscribeMessages(ch, nil)
	go mg.subscriptionMessageHandler(ch)

	var requests []WsRequest

	for _, market := range mg.marketsMap {
		req := WsRequest{
			Op: "subscribe",
			Args: []string{
				fmt.Sprintf("spot/depth%d:%s", MAX_BOOK_DEPTH, market.Symbol),
				fmt.Sprintf("spot/trade:%s", market.Symbol),
			},
		}

		requests = append(requests, req)
	}

	err = ws.RequestSubscriptions(requests)
	if err != nil {
		return err
	}

	return nil
}

var orderbookRegex = regexp.MustCompile(`spot/depth`)
var tradesRegex = regexp.MustCompile(`spot/trade`)

func (mg *MarketDataGateway) subscriptionMessageHandler(ch chan WsResponse) {
	for msg := range ch {
		switch {
		case orderbookRegex.MatchString(msg.Table):
			if err := mg.processOrderBook(msg); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Table, err)
			}
		case tradesRegex.MatchString(msg.Table):
			if err := mg.processTrades(msg); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Table, err)
			}
		}
	}
}

func (mg *MarketDataGateway) processOrderBook(msg WsResponse) error {
	var eventLogs []gateway.Event

	orderBooks := []WsOrderBook{}
	err := json.Unmarshal(msg.Data, &orderBooks)
	if err != nil {
		return fmt.Errorf("%s - Failed to unmarshal order book. Err: %s", Exchange.Name, err)
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

	for _, orderBook := range orderBooks {
		// This is not specified, but seems like, Bitmart started always sending a full book
		// snapshot. Before they were sending incremental updates, so this behaviour might change.
		eventLogs = append(eventLogs, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: orderBook.Symbol,
			},
		})

		appendEvents(orderBook.Symbol, gateway.Bid, orderBook.Bids)
		appendEvents(orderBook.Symbol, gateway.Ask, orderBook.Asks)
	}

	mg.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func (mg *MarketDataGateway) processTrades(msg WsResponse) error {
	trades := []WsTrade{}

	if err := json.Unmarshal(msg.Data, &trades); err != nil {
		return fmt.Errorf("%s - Failed to unmarshal trades. Err: %s", Exchange.Name, err)
	}

	eventLogs := make([]gateway.Event, len(trades))

	for i, trade := range trades {
		var direction gateway.Side
		if trade.Side == "sell" {
			direction = gateway.Ask
		} else {
			direction = gateway.Bid
		}

		eventLogs[i] = gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Timestamp: time.Unix(0, trade.Timestamp),
				Symbol:    trade.Symbol,
				Direction: direction,
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
