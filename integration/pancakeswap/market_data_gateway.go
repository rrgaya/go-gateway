package pancakeswap

import (
	"log"
	"math"
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
		api:     NewAPI(),
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
	log.Printf("PancakeSwap polling for %d order book updates every %v", len(markets), refreshInterval)

	go func() {
		for {
			err := g.updateBooksFromAPI(markets)
			if err != nil {
				log.Printf("PancakeSwap market data failed to fetch order book for markets [%s], err: %s", markets, err)
				time.Sleep(5 * time.Second)
				continue
			}

			time.Sleep(refreshInterval)
		}
	}()

	return nil
}

func (g *MarketDataGateway) updateBooksFromAPI(markets []gateway.Market) error {
	res := struct {
		Datas []PairHourGraph `json:"pairHourDatas"`
	}{}

	ids := make([]string, 0)
	for _, market := range markets {
		ids = append(ids, market.Symbol)
	}

	query := buildQueryForMarkets(ids)
	err := g.api.RunGraphQuery(query, &res)
	if err != nil {
		log.Printf("PancakeSwap failed to update markets, err: %s", err)
		return err
	}

	events := make([]gateway.Event, 0)

	for _, pairHour := range res.Datas {
		pair := pairHour.Pair
		priceTick := 1 / math.Pow10(int(pair.Token1.Decimals)/3)

		events = append(events, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: pair.ID,
			},
		})
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: pair.ID,
				Side:   gateway.Ask,
				Price:  pair.Token1Price + priceTick,
				Amount: pair.Token0.TotalLiquidity,
			},
		})
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: pair.ID,
				Side:   gateway.Bid,
				Price:  pair.Token1Price - priceTick,
				Amount: pair.Token0.TotalLiquidity,
			},
		})

	}

	tick := gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	g.tickCh <- tick

	return nil
}
