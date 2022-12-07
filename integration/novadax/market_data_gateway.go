package novadax

import (
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options gateway.Options
	tickCh  chan gateway.Tick
	api     *API
}

func NewMarketDataGateway(opts gateway.Options, tickCh chan gateway.Tick, api *API) *MarketDataGateway {
	return &MarketDataGateway{
		options: opts,
		tickCh:  tickCh,
		api:     api,
	}
}

func (mg *MarketDataGateway) Connect() error {
	return nil
}

func (mg *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	wg := &sync.WaitGroup{}
	wg.Add(len(markets))

	for _, market := range markets {
		go func(market gateway.Market) {
			mg.sendOrderBookSnapshots(market)

			ws := NewWsSession(mg.tickCh)
			ws.Connect(market)

			wg.Done()
		}(market)
	}

	wg.Wait()
	return nil
}

func (mg *MarketDataGateway) sendOrderBookSnapshots(market gateway.Market) error {
	orderBookDepth := 100
	paramsReq := ApiOrderBookRequest{
		Limit:  orderBookDepth,
		Symbol: market.Symbol,
	}

	book, err := mg.api.GetOrderBook(paramsReq)
	if err != nil {
		return err
	}

	events := make([]gateway.Event, 0)
	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	events = AppendBookEvent(
		book.Bids,
		market.Symbol,
		gateway.Bid,
		events,
	)

	events = AppendBookEvent(
		book.Asks,
		market.Symbol,
		gateway.Ask,
		events,
	)

	mg.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	return nil
}
