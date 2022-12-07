package latoken

import (
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	depthUpdateCh chan DepthUpdate
	tickCh        chan gateway.Tick
	ws            *WsSession
}

func NewMarketDataGateway(depthUpdateCh chan DepthUpdate, ws *WsSession, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		depthUpdateCh: depthUpdateCh,
		tickCh:        tickCh,
		ws:            ws,
	}
}

func (g *MarketDataGateway) Connect() error {
	// Handle depth update from websocket
	go func() {
		for {
			depthUpdate, ok := <-g.depthUpdateCh
			if !ok {
				// channel closing
				break
			}

			g.processDepthUpdate(depthUpdate)
		}
	}()

	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	return g.ws.connectMarketDataInBatches(markets, 100)
}

func (g *MarketDataGateway) processDepthUpdate(depthUpdate DepthUpdate) {
	events := make([]gateway.Event, 0, len(depthUpdate.Bids)+len(depthUpdate.Asks))

	for _, bid := range depthUpdate.Bids {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: depthUpdate.Market.Symbol,
				Side:   gateway.Bid,
				Price:  bid.Price,
				Amount: bid.Quantity,
			},
		})
	}

	for _, ask := range depthUpdate.Asks {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: depthUpdate.Market.Symbol,
				Side:   gateway.Ask,
				Price:  ask.Price,
				Amount: ask.Quantity,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}
