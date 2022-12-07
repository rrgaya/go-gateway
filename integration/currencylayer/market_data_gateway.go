package currencylayer

import (
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	api             *API
	options         gateway.Options
	markets         []gateway.Market
	marketsMap      map[string]gateway.Market
	refreshInterval time.Duration
	tickCh          chan gateway.Tick
}

func NewMarketDataGateway(markets []gateway.Market, api *API, options gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	marketsMap := make(map[string]gateway.Market)
	for _, market := range markets {
		marketsMap[market.Symbol] = market
	}

	refreshInterval := time.Duration(options.RefreshIntervalMs) * time.Millisecond
	if refreshInterval < 1*time.Second {
		log.Printf("CurrencyLayer market data refreshInterval cannot be less than 1 second, defaulting to 300 seconds")
		refreshInterval = 300 * time.Second
	}

	return &MarketDataGateway{
		api:             api,
		options:         options,
		markets:         markets,
		marketsMap:      marketsMap,
		refreshInterval: refreshInterval,
		tickCh:          tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	err := g.refreshQuotes()
	if err != nil {
		return err
	}

	// Refresh quotes every 60 seconds
	go func() {
		for {
			time.Sleep(g.refreshInterval)

			err := g.refreshQuotes()
			if err != nil {
				panic(fmt.Errorf("CurrencyLayer failed to refresh quotes, err: %s", err))
			}
		}
	}()

	return nil
}

func (g *MarketDataGateway) refreshQuotes() error {
	quotes, err := g.api.GetQuotes(g.options.CurrencyLayerSource)
	if err != nil {
		return err
	}

	for _, quote := range quotes {
		market, ok := g.marketsMap[quote.Symbol]
		if !ok {
			log.Printf("CurrencyLayer failed to find %s quote symbol in marketsMap, probably need to restart", quote.Symbol)
			continue
		}

		g.quoteUpdate(market, quote)
	}

	return nil
}

func (g *MarketDataGateway) quoteUpdate(market gateway.Market, quote Quote) {
	events := make([]gateway.Event, 0, 3)

	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	events = append(events, gateway.Event{
		Type: gateway.DepthEvent,
		Data: gateway.Depth{
			Symbol: market.Symbol,
			Side:   gateway.Bid,
			Price:  quote.Rate,
			Amount: 999999999,
		},
	})

	events = append(events, gateway.Event{
		Type: gateway.DepthEvent,
		Data: gateway.Depth{
			Symbol: market.Symbol,
			Side:   gateway.Ask,
			Price:  quote.Rate + market.PriceTick,
			Amount: 999999999,
		},
	})

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}
