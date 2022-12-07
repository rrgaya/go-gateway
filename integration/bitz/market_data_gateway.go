package bitz

import (
	"fmt"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	gateway *Gateway
	tickCh  chan gateway.Tick
}

func NewMarketDataGateway(gtw *Gateway) *MarketDataGateway {
	return &MarketDataGateway{
		gateway: gtw,
		tickCh:  gtw.tickCh,
	}
}

func (g *MarketDataGateway) Connect(ws *WS, markets []gateway.Market) error {
	ws.depthUpdate = make(chan DepthUpdate)
	go g.depthUpdateHandler(ws.depthUpdate, markets)

	err := g.subscribeMarketDepths(ws, markets)
	if err != nil {
		return err
	}

	return nil
}

func (g *MarketDataGateway) depthUpdateHandler(depthUpdateCh chan DepthUpdate, markets []gateway.Market) {
	marketsMap := make(map[string]gateway.Market)
	for _, market := range markets {
		marketsMap[market.Symbol] = market
	}

	for update := range depthUpdateCh {
		//log.Printf("bitz received market depth update %s bids %d asks %d", market, len(bids), len(asks))

		tick := bidAsksUpdateToTick(update, false)
		go g.dispatchTickUpdate(tick)
	}
}

func (g *MarketDataGateway) subscribeMarketDepths(ws *WS, markets []gateway.Market) error {
	wg := sync.WaitGroup{}
	wg.Add(len(markets))
	subErrs := make(chan error, len(markets))

	for _, market := range markets {
		go func(market gateway.Market) {
			err := g.subscribeMarketDepth(ws, market)
			if err != nil {
				subErrs <- err
			}
			wg.Done()
		}(market)
	}

	wg.Wait()

	if len(subErrs) > 0 {
		errCount := len(subErrs)
		firstErr := <-subErrs
		return fmt.Errorf("failed to subscribe to market depths, %d errors, first err: %s", errCount, firstErr)
	}

	return nil
}

func (g *MarketDataGateway) subscribeMarketDepth(ws *WS, market gateway.Market) error {
	update, err := ws.SubscribeMarketDepth(market.Symbol)
	if err != nil {
		return err
	}

	//log.Printf("bitz received market depth snapshot %s bids %d asks %d", update.Symbol, len(bids), len(asks))

	tick := bidAsksUpdateToTick(update, true)
	go g.dispatchTickUpdate(tick)

	return nil
}

func (g *MarketDataGateway) dispatchTickUpdate(tick gateway.Tick) {
	g.tickCh <- tick
}

func bidAsksUpdateToTick(update DepthUpdate, snapshot bool) gateway.Tick {
	eventLog := make([]gateway.Event, 0)

	if snapshot {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: update.Symbol,
			},
		})
	}

	for _, ask := range update.Asks {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: update.Symbol,
				Side:   gateway.Ask,
				Price:  ask.Price,
				Amount: ask.Amount,
			},
		})
	}
	for _, bid := range update.Bids {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: update.Symbol,
				Side:   gateway.Bid,
				Price:  bid.Price,
				Amount: bid.Amount,
			},
		})
	}

	return gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}
}
