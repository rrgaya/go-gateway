package bigone

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	api     APIClient
	options gateway.Options
	tickCh  chan gateway.Tick
}

func NewMarketDataGateway(api APIClient, options gateway.Options, markets []gateway.Market, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		api:     api,
		options: options,
		tickCh:  tickCh,
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 50
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	for index, market := range markets {
		group := index / batchesOf

		if batches[group] == nil {
			batches[group] = make([]gateway.Market, 0)
		}

		batches[group] = append(batches[group], market)
	}

	log.Printf("BigONE has [%d] markets, will need to distribute market data subscriptions in %d websocket connections, maximum of [%d] subscriptions on each websocket.", len(markets), len(batches), batchesOf)

	for _, batch := range batches {
		err := g.connectWs(batch, nil, 3, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *MarketDataGateway) connectWs(markets []gateway.Market, proxy *url.URL, maxRetries, retryCount int) error {
	onDisconnect := make(chan error)
	ws := NewWsSession(g.options, onDisconnect)

	if proxy != nil {
		ws.SetProxy(proxy)
	}

	retryCount = 0
	go func() {
		err := <-onDisconnect
		if g.options.Verbose {
			log.Printf("BigONE market data gateway ws disconnected, err: %s", err)
			log.Printf("BigONE reconnecting in 5 seconds...")
		}
		time.Sleep(3 * time.Second)
		err = g.connectWs(markets, proxy, maxRetries, retryCount)
		if err != nil {
			err = fmt.Errorf("BigONE failed to reconnect after disconnect, err: %s", err)
			panic(err)
		}
	}()

	err := g.connectWsAndSubscribeMarketData(ws, markets)
	if err != nil {
		log.Printf("BigONE ws [%s] [%s] failed to connect and subscribe to market data [retry count %d], err: %s", proxy, markets, retryCount, err)
		if retryCount > maxRetries {
			err = fmt.Errorf("Reached maximum connect retry count of %d, err: %s", maxRetries, err)
			return err
		}
		time.Sleep(3 * time.Second)
		return g.connectWs(markets, proxy, maxRetries, retryCount+1)
	}

	return nil
}

func (g *MarketDataGateway) connectWsAndSubscribeMarketData(ws *WsSession, markets []gateway.Market) error {
	err := ws.Connect()
	if err != nil {
		return err
	}

	err = g.subscribeDepth(ws, markets)
	if err != nil {
		return err
	}

	return nil
}

func (g *MarketDataGateway) subscribeDepth(ws *WsSession, markets []gateway.Market) error {
	resChs := make(map[gateway.Market]chan []byte)
	for _, mkt := range markets {
		subReq := WsRequest{
			SubscribeMarketDepthRequest: &WsMarketRequest{
				Market: mkt.Symbol,
			},
		}

		resCh, err := ws.SendRequestAsync(&subReq)
		if err != nil {
			return err
		}

		resChs[mkt] = resCh
	}

	for market, resCh := range resChs {
		go func(market gateway.Market, resCh chan []byte) {
			for msg := range resCh {
				_, dataType, _, _ := jsonparser.Get(msg, "error")
				if dataType != jsonparser.NotExist {
					log.Printf("Depth update mkt [%s] returned err [%s]", market, string(msg))
					continue
				}

				err := g.processDepthUpdate(msg)
				if err != nil {
					log.Printf("Failed to process mkt data msg [%s] for market [%s], err [%s]", msg, market, err)
				}
			}
		}(market, resCh)
	}

	return nil
}

func (g *MarketDataGateway) processDepthUpdate(msg []byte) error {
	var snapshot bool

	dataVal, dataType, _, _ := jsonparser.Get(msg, "depthSnapshot")
	if dataType != jsonparser.NotExist {
		snapshot = true
	} else {
		dataVal, _, _, _ = jsonparser.Get(msg, "depthUpdate")
	}

	depthVal, dataType, _, _ := jsonparser.Get(dataVal, "depth")
	if dataType == jsonparser.NotExist {
		return fmt.Errorf("expected json msg [%s] to have json depth key data", string(msg))
	}

	var depth WsDepth
	err := json.Unmarshal(depthVal, &depth)
	if err != nil {
		return err
	}

	symbol := depth.Market
	events := make([]gateway.Event, 0, len(depth.Bids)+len(depth.Asks)+1)

	if snapshot {
		events = append(events, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: symbol,
			},
		})
	}

	for _, price := range depth.Bids {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   gateway.Bid,
				Price:  price.Price,
				Amount: price.Amount,
			},
		})
	}
	for _, price := range depth.Asks {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   gateway.Ask,
				Price:  price.Price,
				Amount: price.Amount,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	return nil
}
