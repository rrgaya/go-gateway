package coinbene

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	gtw    *Gateway
	tickCh chan gateway.Tick
}

func NewMarketDataGateway(gtw *Gateway, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		gtw:    gtw,
		tickCh: tickCh,
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	bookCh := make(chan BookUpdate, 100)

	// Group markets into batches
	batchesOf := 25
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)
	for index, market := range markets {
		group := index / batchesOf
		batches[group] = append(batches[group], market)
	}

	log.Printf("Coinbene subscribing to %d markets, will need to distribute market data subscriptions in %d websocket connections, maximum of %d subscriptions on each websocket.", len(markets), len(batches), batchesOf)

	wsDone := make(chan bool)
	wsError := make(chan error)
	wsQuit := make([]chan bool, len(batches))

	wsWg := sync.WaitGroup{}
	wsWg.Add(len(batches))

	for index, batch := range batches {
		wsQuit[index] = make(chan bool)

		go func(batch []gateway.Market, quit chan bool) {
			ws := NewWsSession()
			err := ws.Connect()
			if err != nil {
				wsError <- fmt.Errorf("Failed to connect to open ws con for market data, err: %s", err)
				return
			}

			err = g.subscribeMarketData(ws, batch, bookCh)
			if err != nil {
				ws.Close()
				wsError <- fmt.Errorf("Failed to subscribe to market data, err: %s", err)
				return
			}

			wsWg.Done()

			shouldQuit := <-quit
			if shouldQuit {
				ws.Close()
			}
		}(batch, wsQuit[index])
	}

	go func() {
		wsWg.Wait()
		wsDone <- true
	}()

	var err error
	select {
	case <-wsDone:
		log.Printf("Coinbene finished opening %d ws connections", len(batches))
	case wsErr := <-wsError:
		err = wsErr
		log.Printf("Coinbene failed to open ws, err: %s", wsErr)
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
		go g.bookMessageHandler(bookCh)
	}

	return err
}

func (g *MarketDataGateway) subscribeMarketData(ws *WsSession, markets []gateway.Market, bookCh chan BookUpdate) error {
	for _, market := range markets {
		err := ws.SubscribeBook(market, bookCh, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *MarketDataGateway) bookMessageHandler(bookCh chan BookUpdate) {
	for book := range bookCh {
		eventLog := make([]gateway.Event, 0)

		if book.Snapshot {
			eventLog = append(eventLog, gateway.Event{
				Type: gateway.SnapshotSequenceEvent,
				Data: gateway.SnapshotSequence{
					Symbol: book.Market.Symbol,
				},
			})
		}

		for _, ask := range book.Asks {
			eventLog = append(eventLog, gateway.Event{
				Type: gateway.DepthEvent,
				Data: gateway.Depth{
					Symbol: book.Market.Symbol,
					Side:   gateway.Ask,
					Price:  ask.Price,
					Amount: ask.Amount,
				},
			})
		}
		for _, bid := range book.Bids {
			eventLog = append(eventLog, gateway.Event{
				Type: gateway.DepthEvent,
				Data: gateway.Depth{
					Symbol: book.Market.Symbol,
					Side:   gateway.Bid,
					Price:  bid.Price,
					Amount: bid.Amount,
				},
			})
		}

		g.tickCh <- gateway.Tick{
			ReceivedTimestamp: time.Now(),
			EventLog:          eventLog,
		}
	}
}
