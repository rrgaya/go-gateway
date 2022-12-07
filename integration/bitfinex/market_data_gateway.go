package bitfinex

import (
	"context"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"

	bfxbook "github.com/bitfinexcom/bitfinex-api-go/pkg/models/book"
	bfxcommon "github.com/bitfinexcom/bitfinex-api-go/pkg/models/common"
	bfxtrade "github.com/bitfinexcom/bitfinex-api-go/pkg/models/trade"
	"github.com/bitfinexcom/bitfinex-api-go/v2/rest"
	"github.com/bitfinexcom/bitfinex-api-go/v2/websocket"
)

type MarketDataGateway struct {
	options      gateway.Options
	tickCh       chan gateway.Tick
	rest         *rest.Client
	ws           *websocket.Client
	orderCbs     map[int64]int64
	orderCbsLock *sync.Mutex
}

func NewMarketDataGateway(options gateway.Options, ws *websocket.Client, rest *rest.Client, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options: options,
		tickCh:  tickCh,
		ws:      ws,
		rest:    rest,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 30
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	log.Printf("Bitfinex has %d markets, will need %d websocket connections (order book + trades), maximum of %d topics on each websocket.", len(markets), len(batches)*2, batchesOf)

	for index, market := range markets {
		group := index / batchesOf
		batches[group] = append(batches[group], market)
	}

	for _, batch := range batches {
		go g.connectWebsocket(batch, g.requestOrderBookSubscriptions)
		go g.connectWebsocket(batch, g.requestTradesSubscriptions)
	}

	return nil
}

func (g *MarketDataGateway) requestOrderBookSubscriptions(ws *websocket.Client, markets []gateway.Market) {
	ctx, cxl1 := context.WithTimeout(context.Background(), time.Second*5)
	defer cxl1()

	log.Printf("Bitfinex websocket subscribing to %v order book topics.", len(markets))

	for _, market := range markets {
		_, err := ws.SubscribeBook(ctx, market.Symbol, bfxcommon.Precision0, bfxcommon.FrequencyRealtime, 25)
		if err != nil {
			panic(err)
		}
	}
}

func (g *MarketDataGateway) requestTradesSubscriptions(ws *websocket.Client, markets []gateway.Market) {
	ctx, cxl1 := context.WithTimeout(context.Background(), time.Second*5)
	defer cxl1()

	log.Printf("Bitfinex websocket subscribing to %v trade topics.", len(markets))

	for _, market := range markets {
		_, err := ws.SubscribeTrades(ctx, market.Symbol)
		if err != nil {
			panic(err)
		}
	}
}

func (g *MarketDataGateway) connectWebsocket(markets []gateway.Market, requestSubscriptions func(*websocket.Client, []gateway.Market)) {
	defer func() {
		err := recover()

		if err != nil {
			log.Printf("Bitfinex websocket error: %s", err)
			log.Printf("Bitfinex websocket connection dropped, reconnecting in 15 seconds...")

			time.Sleep(15 * time.Second)

			g.connectWebsocket(markets, requestSubscriptions)
		}
	}()

	requestSubscriptions(g.ws, markets)
}

func (g *MarketDataGateway) processBookUpdate(data *bfxbook.Book, symbolsToMarket map[string]gateway.Market) {
	market, ok := symbolsToMarket[data.Symbol]
	if !ok {
		log.Printf("Bitfinex book update failed to locate symbol %s on symbolsToMarket", data.Symbol)
		return
	}

	var amount float64
	if data.Count == 0 {
		amount = 0
	} else {
		amount = data.Amount
	}

	var side gateway.Side
	if data.Side == bfxcommon.Bid {
		side = gateway.Bid
	} else {
		side = gateway.Ask
	}

	events := make([]gateway.Event, 1)
	events[0] = gateway.Event{
		Type: gateway.DepthEvent,
		Data: gateway.Depth{
			Symbol: market.Symbol,
			Side:   side,
			Price:  data.Price,
			Amount: amount,
		},
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (g *MarketDataGateway) processBookSnapshot(data *bfxbook.Snapshot, symbolsToMarket map[string]gateway.Market) {
	symbol := data.Snapshot[0].Symbol
	market, ok := symbolsToMarket[symbol]
	if !ok {
		log.Printf("Bitfinex snapshot failed to locate symbol %s on symbolsToMarket", symbol)
		return
	}

	events := make([]gateway.Event, 0, len(data.Snapshot)+1)
	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	for _, data := range data.Snapshot {
		var amount float64
		if data.Count == 0 {
			amount = 0
		} else {
			amount = data.Amount
		}

		var side gateway.Side
		if data.Side == bfxcommon.Bid {
			side = gateway.Bid
		} else {
			side = gateway.Ask
		}

		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   side,
				Price:  data.Price,
				Amount: amount,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (g *MarketDataGateway) processTrade(data *bfxtrade.Trade, symbolsToMarket map[string]gateway.Market) {
	market, ok := symbolsToMarket[data.Pair]
	if !ok {
		log.Printf("Bitfinex trade update failed to locate symbol %s on symbolsToMarket", data.Pair)
		return
	}

	var side gateway.Side
	if data.Amount > 0 {
		side = gateway.Bid
	} else {
		side = gateway.Ask
	}

	events := make([]gateway.Event, 1)
	events[0] = gateway.Event{
		Type: gateway.TradeEvent,
		Data: gateway.Trade{
			ID:        strconv.FormatInt(data.ID, 10),
			Timestamp: time.Unix(0, data.MTS*int64(time.Millisecond)),
			Symbol:    market.Symbol,
			Direction: side,
			Price:     data.Price,
			Amount:    math.Abs(data.Amount),
		},
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (g *MarketDataGateway) Tick() chan gateway.Tick {
	return g.tickCh
}
