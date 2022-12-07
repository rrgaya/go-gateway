package bitmex

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type Trade struct {
	Size            float64   `json:"size"`
	Price           float64   `json:"price"`
	ForeignNotional float64   `json:"foreignNotional"`
	GrossValue      float64   `json:"grossValue"`
	HomeNotional    float64   `json:"homeNotional"`
	Symbol          string    `json:"symbol"`
	TickDirection   string    `json:"tickDirection"`
	Side            string    `json:"side"`
	TradeMatchID    string    `json:"trdMatchID"`
	Timestamp       time.Time `json:"timestamp"`
}

type BookLevel struct {
	Symbol string  `json:"symbol"`
	Id     int64   `json:"id"`
	Side   string  `json:"side"`
	Size   float64 `json:"size"`
	Price  float64 `json:"price"`
}

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

func (g *MarketDataGateway) Connect(ws *WsSession) error {
	g.ws = ws
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	g.marketsMap = make(map[string]gateway.Market)

	for _, market := range markets {
		g.marketsMap[market.Symbol] = market
	}

	log.Printf("Bitmex subscribing to %d markets...", len(markets))

	// TODO: Split more than 3 markets in multiple ws connections
	if len(g.marketsMap) > 3 {
		log.Printf("WARNING bitmex subscribing to more than 3 markets can cause issues!")
	}

	ch := make(chan WsResponse, 10000)
	g.ws.SubscribeMessages(ch, nil)
	go g.subscriptionMessageHandler(ch)

	for symbol, _ := range g.marketsMap {
		topics := []string{
			fmt.Sprintf("orderBookL2:%s", symbol),
			fmt.Sprintf("trade:%s", symbol),
			fmt.Sprintf("instrument:%s", symbol),
		}

		err := g.ws.RequestSubscriptions(topics)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *MarketDataGateway) subscriptionMessageHandler(ch chan WsResponse) {
	// Bitmex doesn't resend the book level price on updates, only when
	// the "order book level" is inserted or when we connect to the websocket
	// during the "insert" and "partial" message actions.
	// Thats why we need to maintain our own state in-memory to get the book
	// level price for given "id".
	// Note, that the book level "id" is actually composited from the book
	// level symbol + the book level price, if we find out the "symbol" int
	// id, we could deduce the price from the book level id.
	bookLevelPrices := make(map[int64]float64)

	for msg := range ch {
		switch msg.Table {
		case "orderBookL2":
			g.processBookLevel2Update(msg, bookLevelPrices)
		case "trade":
			g.processTrades(msg)
		case "instrument":
			g.processInstrument(msg)
		}
	}
}

func bookLevelSideToCommonSide(side string) gateway.Side {
	if side == "Buy" {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func (g *MarketDataGateway) processBookLevel2Update(msg WsResponse, bookLevelPrices map[int64]float64) {
	eventLogs := make(map[string][]gateway.Event)
	snapshotSequence := make(map[string]bool)

	var bookLevels []BookLevel
	err := json.Unmarshal(msg.Data, &bookLevels)
	if err != nil {
		log.Printf("Failed to unmarshal bitmex book levels, data: %v, err: %s", string(msg.Data), err)
		return
	}

	for _, bookLevel := range bookLevels {
		// Initialize event log for this symbol if necessary
		_, ok := eventLogs[bookLevel.Symbol]
		if !ok {
			eventLogs[bookLevel.Symbol] = make([]gateway.Event, 0)
		}

		switch msg.Action {
		case "partial":
			bookLevelPrices[bookLevel.Id] = bookLevel.Price
			snapshotSequence[bookLevel.Symbol] = true // Flag that we are receiving a snapshot sequence now
		case "insert":
			bookLevelPrices[bookLevel.Id] = bookLevel.Price
		case "update":
			price, ok := bookLevelPrices[bookLevel.Id]
			if !ok {
				log.Printf("Expected bookLevelPrices to contain book level ID %d from \"update\" action", bookLevel.Id)
				continue
			}

			bookLevel.Price = price
		case "delete":
			price, ok := bookLevelPrices[bookLevel.Id]
			if !ok {
				log.Printf("Expected bookLevelPrices to contain book level ID %d from \"delete\" action", bookLevel.Id)
				continue
			}

			bookLevel.Price = price
			delete(bookLevelPrices, bookLevel.Id)
		}

		amount := bookLevel.Size
		if msg.Action == "delete" {
			amount = 0
		}

		eventLogs[bookLevel.Symbol] = append(eventLogs[bookLevel.Symbol], gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: bookLevel.Symbol,
				Side:   bookLevelSideToCommonSide(bookLevel.Side),
				Price:  bookLevel.Price,
				Amount: amount,
			},
		})
	}

	// Merge eventLogs int single eventLog
	eventLog := make([]gateway.Event, 0)
	for symbol, events := range eventLogs {
		// Check if received a snapshot sequence
		_, ok := snapshotSequence[symbol]
		if ok {
			eventLog = append(eventLog, gateway.Event{
				Type: gateway.SnapshotSequenceEvent,
				Data: gateway.SnapshotSequence{
					Symbol: symbol,
				},
			})
		}

		eventLog = append(eventLog, events...)
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}
}

func (g *MarketDataGateway) processTrades(msg WsResponse) {
	// We only want to process new trades, we don't care about receiving past trades
	// from the "snapshot" sequence
	if msg.Action != "insert" {
		return
	}

	var trades []Trade
	err := json.Unmarshal(msg.Data, &trades)
	if err != nil {
		log.Printf("Failed to unmarshal bitmex trades, data: %s, err :%s", string(msg.Data), err)
		return
	}

	eventLog := make([]gateway.Event, len(trades))
	for i, trade := range trades {
		eventLog[i] = gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				ID:        trade.TradeMatchID,
				Timestamp: trade.Timestamp,
				Symbol:    trade.Symbol,
				Direction: bookLevelSideToCommonSide(trade.Side),
				Price:     trade.Price,
				Amount:    trade.Size,
			},
		}
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}
}

func (g *MarketDataGateway) processInstrument(msg WsResponse) {
	var res []gateway.BitmexInstrument
	err := json.Unmarshal(msg.Data, &res)
	if err != nil {
		log.Printf("Failed to unmarshal bitmex instrument, data: %s, err :%s", string(msg.Data), err)
		return
	}

	eventLog := make([]gateway.Event, len(res))
	for i, instrument := range res {
		eventLog[i] = gateway.Event{
			Type: gateway.BitmexInstrumentEvent,
			Data: instrument,
		}
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}
}
