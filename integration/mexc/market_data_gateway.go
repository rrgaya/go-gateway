package mexc

import (
	"fmt"
	"log"
	"net/url"
	"sort"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options        gateway.Options
	api            *API
	tickCh         chan gateway.Tick
	tradesFromTime int64
}

func NewMarketDataGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options: options,
		api:     NewAPI(options),
		tickCh:  tickCh,
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	var refreshInterval time.Duration
	if g.options.RefreshIntervalMs > 0 {
		refreshInterval = time.Duration(g.options.RefreshIntervalMs * int(time.Millisecond))
	} else {
		refreshInterval = 2500 * time.Millisecond
	}

	// Subscribe to market updates
	log.Printf("MEXC polling for %d order book updates every %v", len(markets), refreshInterval)

	for _, market := range markets {
		go func(market gateway.Market) {
			for {
				err := g.updateBooksFromAPI(market)
				if err != nil {
					log.Printf("MEXC market data failed to fetch order book %s, err: %s", market.Symbol, err)
					time.Sleep(5 * time.Second)
					continue
				}

				time.Sleep(refreshInterval)
			}
		}(market)
		go func(market gateway.Market) {
			for {
				err := g.updateTradesFromAPI(market)
				if err != nil {
					log.Printf("MEXC market data failed to fetch trades %s, err: %s", market.Symbol, err)
					time.Sleep(5 * time.Second)
					continue
				}

				time.Sleep(refreshInterval)
			}
		}(market)
	}

	return nil
}

func (g *MarketDataGateway) updateTradesFromAPI(market gateway.Market) (err error) {
	params := &url.Values{}
	params.Set("symbol", market.Symbol)

	trades, err := g.api.Trades(params)
	if err != nil {
		return
	}

	g.processAPITradesUpdate(market, trades)

	return nil
}

func (g *MarketDataGateway) processAPITradesUpdate(market gateway.Market, trades []APITrade) {
	events := make([]gateway.Event, 0, len(trades))

	// Trades in asending order
	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Time < trades[j].Time
	})

	oldestTradeTime := g.tradesFromTime

	for _, t := range trades {
		var side gateway.Side
		if t.IsBuyerMaker {
			side = gateway.Ask
		} else {
			side = gateway.Bid
		}

		if t.Time > g.tradesFromTime {
			events = append(events, gateway.Event{
				Type: gateway.TradeEvent,
				Data: gateway.Trade{
					Timestamp: time.UnixMilli(t.Time),
					Symbol:    market.Symbol,
					ID:        fmt.Sprintf("%s%d%f%f", string(side[0]), t.Time, t.Price, t.Qty),
					Direction: side,
					Price:     t.Price,
					Amount:    t.Qty,
				},
			})

			oldestTradeTime = t.Time
		}
	}

	g.tradesFromTime = oldestTradeTime

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (g *MarketDataGateway) updateBooksFromAPI(market gateway.Market) error {
	params := &url.Values{}
	params.Set("symbol", market.Symbol)

	res, err := g.api.Depth(params)
	if err != nil {
		return err
	}

	g.processAPIOrderBookUpdate(market, res)
	return nil
}

func (g *MarketDataGateway) apiBookRowToGtwEvent(side gateway.Side, symbol string, px gateway.PriceArray) gateway.Event {
	return gateway.Event{
		Type: gateway.DepthEvent,
		Data: gateway.Depth{
			Symbol: symbol,
			Side:   side,
			Price:  px.Price,
			Amount: px.Amount,
		},
	}
}

func (g *MarketDataGateway) processAPIOrderBookUpdate(market gateway.Market, book APIDepth) {
	events := make([]gateway.Event, 0, len(book.Bids)+len(book.Asks)+1)
	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	for _, p := range book.Bids {
		events = append(events, g.apiBookRowToGtwEvent(gateway.Bid, market.Symbol, p))
	}
	for _, p := range book.Asks {
		events = append(events, g.apiBookRowToGtwEvent(gateway.Ask, market.Symbol, p))
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}
