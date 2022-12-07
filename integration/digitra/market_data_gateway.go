package digitra

import (
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options gateway.Options
	api     *API
	tickCh  chan gateway.Tick
}

func NewMarketDataGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options: options,
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
	log.Printf("Digitra polling for %d order book updates every %v", len(markets), refreshInterval)

	for _, market := range markets {
		go func(market gateway.Market) {
			for {
				err := g.updateBooksFromAPI(market)
				if err != nil {
					log.Printf("Digitra market data failed to fetch order book %s, err: %s", market.Symbol, err)
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
	params := make(map[string]interface{})
	params["expand"] = "ORDER_BOOK"

	res, err := g.api.Market(market.Symbol, params)
	if err != nil {
		return err
	}

	return g.processAPIBookOrders(market, res.OrderBook)
}

func (g *MarketDataGateway) processAPIBookOrders(market gateway.Market, depth APIOrderBook) error {
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

func bookOrderToGtwEvent(symbol string, side gateway.Side, p APIOrderBookPrice) gateway.Event {
	return gateway.Event{
		Type: gateway.DepthEvent,
		Data: gateway.Depth{
			Symbol: symbol,
			Side:   side,
			Price:  p.Price,
			Amount: p.Size,
		},
	}
}
