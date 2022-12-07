package ripio

import (
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options gateway.Options
	markets []gateway.Market
	api     *Api
	tickCh  chan gateway.Tick
}

func NewMarketDataGateway(options gateway.Options, markets []gateway.Market, api *Api, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options: options,
		markets: markets,
		api:     api,
		tickCh:  tickCh,
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	// Subscribe to market updates
	log.Printf("Ripio polling for %d order book updates", len(markets))

	for _, market := range markets {
		go func(market gateway.Market) {
			for {
				orderBookRes, err := g.api.GetOrderBook(market.Symbol)
				if err != nil {
					log.Printf("Ripio market data failed to fetch order book %s, err: %s", market.Symbol, err)
					continue
				}

				g.processOrderBookUpdate(market, orderBookRes)

				time.Sleep(10 * time.Second)
			}
		}(market)
	}

	return nil
}

func (g *MarketDataGateway) processOrderBookUpdate(market gateway.Market, update OrderBookRes) {
	events := make([]gateway.Event, 0, len(update.Buy)+len(update.Sell)+1)

	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	for _, p := range update.Buy {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   gateway.Bid,
				Price:  p.Price,
				Amount: p.Amount,
			},
		})
	}
	for _, p := range update.Sell {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   gateway.Ask,
				Price:  p.Price,
				Amount: p.Amount,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}
