package bitforex

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options         gateway.Options
	markets         []gateway.Market
	symbolsToMarket map[string]gateway.Market
	tickCh          chan gateway.Tick
}

func NewMarketDataGateway(options gateway.Options, markets []gateway.Market, tickCh chan gateway.Tick) *MarketDataGateway {
	symbolsToMarket := make(map[string]gateway.Market)

	for _, market := range markets {
		symbolsToMarket[market.Symbol] = market
	}

	return &MarketDataGateway{
		options:         options,
		markets:         markets,
		symbolsToMarket: symbolsToMarket,
		tickCh:          tickCh,
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	messageCh := make(chan WsMessage)

	batchesOf := 100
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)
	for index, market := range markets {
		group := index / batchesOf
		batches[group] = append(batches[group], market)
	}

	log.Printf("Bitforex connecting to %d markets, will need to distribute market data subscriptions in %d connections per group, maximum of %d subscriptions.", len(markets), len(batches), batchesOf)

	wsDone := make(chan bool)
	wsError := make(chan error)
	wsQuit := make([]chan bool, len(batches))

	wsWg := sync.WaitGroup{}
	wsWg.Add(len(batches))

	var index int
	for _, batch := range batches {
		wsQuit[index] = make(chan bool)

		go func(batch []gateway.Market, quit chan bool) {
			ws := NewWsSession(g.options, messageCh)
			err := ws.Connect(WsMktApiCoinGroup1)
			if err != nil {
				wsError <- fmt.Errorf("Failed to connect to market data (coinGroup %s), err: %s", WsMktApiCoinGroup1, err)
				return
			}

			err = g.subscribeMarketDepth(ws, batch)
			if err != nil {
				ws.Close()
				wsError <- fmt.Errorf("Failed to subscribe to markets depth, err: %s", err)
				return
			}

			wsWg.Done()

			shouldQuit := <-quit
			if shouldQuit {
				ws.Close()
			}

		}(batch, wsQuit[index])

		index++
	}

	go func() {
		wsWg.Wait()
		wsDone <- true
	}()

	var err error
	select {
	case <-wsDone:
		log.Printf("Finished opening %d ws connections", len(batches))
	case wsErr := <-wsError:
		err = wsErr
		log.Printf("Bitforex failed to open ws, err: %s", wsErr)
		log.Printf("Closing all ws connections...")
	}

	hasErr := err != nil
	for _, quit := range wsQuit {
		select {
		case quit <- hasErr:
		default:
			// Doesn't need to quit, never connected
		}
	}

	if !hasErr {
		go g.messageHandler(messageCh)
	}

	return err
}

func (g *MarketDataGateway) subscribeMarketDepth(ws *WsSession, markets []gateway.Market) error {
	for _, market := range markets {
		request, err := g.buildMarketDepthRequest(market)
		if err != nil {
			return fmt.Errorf("failed to build market depth request for market %s, err: %s", market, err)
		}

		err = ws.SendRequests([]WsRequest{request})
		if err != nil {
			return fmt.Errorf("failed to send market depth request for market %s, err: %s", market, err)
		}
	}

	return nil
}

func (g *MarketDataGateway) buildMarketDepthRequest(market gateway.Market) (WsRequest, error) {
	param := WsDepthSubParam{
		BusinessType: market.Symbol,
		DType:        0,
		Size:         100,
	}

	return NewWsRequest("subHq", "depth10", param)
}

func (g *MarketDataGateway) messageHandler(wsMessage chan WsMessage) {
	for {
		message, ok := <-wsMessage
		if !ok {
			// Channel closed
			return
		}

		if message.Event == "depth10" {
			err := g.processDepthUpdate(message)
			if err != nil {
				log.Printf("Failed to process depth update, err: %s", err)
			}
		}
	}
}

func (g *MarketDataGateway) processDepthUpdate(message WsMessage) error {
	var data WsDepthUpdate
	var param WsDepthSubParam

	err := json.Unmarshal(message.Data, &data)
	if err != nil {
		return err
	}
	err = json.Unmarshal(message.Param, &param)
	if err != nil {
		return err
	}

	// Ignore this depth update, we are probably receiving an empty depth update
	// from a ws "coin group" connection that doesn't send updates for the coin we requested.
	if len(data.Bids) == 0 && len(data.Asks) == 0 {
		return nil
	}

	market, ok := g.symbolsToMarket[param.BusinessType]
	if !ok {
		return fmt.Errorf("unable to locate market for businessType %s", param.BusinessType)
	}

	//log.Printf("Processed depth update %s, bids %d, asks %d", market, len(data.Bids), len(data.Asks))

	events := make([]gateway.Event, 0, len(data.Bids)+len(data.Asks)+1)

	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	for _, bid := range data.Bids {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   gateway.Bid,
				Price:  bid.Price,
				Amount: bid.Amount,
			},
		})
	}

	for _, ask := range data.Asks {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   gateway.Ask,
				Price:  ask.Price,
				Amount: ask.Amount,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	return nil
}

func (g *MarketDataGateway) Tick() chan gateway.Tick {
	return g.tickCh
}
