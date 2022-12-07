package coinstore

import (
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options gateway.Options
	markets []gateway.Market
	api     *API
	tickCh  chan gateway.Tick
}

func NewMarketDataGateway(options gateway.Options, markets []gateway.Market, api *API, tickCh chan gateway.Tick) *MarketDataGateway {
	symbolsToMarket := make(map[string]gateway.Market)

	for _, market := range markets {
		symbolsToMarket[market.Symbol] = market
	}

	return &MarketDataGateway{
		options: options,
		markets: markets,
		api:     api,
		tickCh:  tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	var refreshInterval time.Duration
	if g.options.RefreshIntervalMs > 0 {
		refreshInterval = time.Duration(g.options.RefreshIntervalMs * int(time.Millisecond))
	} else {
		refreshInterval = 2500 * time.Millisecond
	}

	// Subscribe to market updates
	log.Printf("CoinStore polling for %d order book updates every %v", len(markets), refreshInterval)

	for _, market := range markets {
		go func(market gateway.Market) {
			for {
				err := g.updateBooksFromAPI(market)
				if err != nil {
					log.Printf("CoinStore market data failed to fetch order book %s, err: %s", market.Symbol, err)
					time.Sleep(5 * time.Second)
					continue
				}

				time.Sleep(refreshInterval)
			}
		}(market)
	}

	return nil
}

func (g *MarketDataGateway) updateBooksFromAPI(market gateway.Market) error {
	params := make(map[string]string)
	params["market_pair"] = market.Symbol
	params["level"] = "3"
	params["depth"] = "100"

	res, err := g.api.Depth(params)
	if err != nil {
		return err
	}

	return g.processAPIBookOrders(market, res)
}

func (g *MarketDataGateway) processAPIBookOrders(market gateway.Market, depth APIDepth) error {
	events := make([]gateway.Event, 0, len(depth.Bids)+len(depth.Asks))
	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	for _, p := range depth.Bids {
		events = append(events, bookOrderToGtwEvent(market.Symbol, gateway.Bid, p))
	}
	for _, p := range depth.Asks {
		events = append(events, bookOrderToGtwEvent(market.Symbol, gateway.Ask, p))
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	return nil
}

func bookOrderToGtwEvent(symbol string, side gateway.Side, p gateway.PriceArray) gateway.Event {
	return gateway.Event{
		Type: gateway.DepthEvent,
		Data: gateway.Depth{
			Symbol: symbol,
			Side:   side,
			Price:  p.Price,
			Amount: p.Amount,
		},
	}
}
