package digifinex

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

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

	log.Printf("DigiFinex has [%d] markets, will need to distribute market data subscriptions in %d websocket connections, maximum of [%d] subscriptions on each websocket.", len(markets), len(batches), batchesOf)

	for _, batch := range batches {
		err := g.connectWs(batch, nil, 3, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *MarketDataGateway) connectWs(markets []gateway.Market, proxy *url.URL, maxRetries, retryCount int) error {
	ws := NewWsSession(g.options)

	if proxy != nil {
		ws.SetProxy(proxy)
	}

	err := g.connectWsAndSubscribeMarketData(ws, markets)
	if err != nil {
		log.Printf("DigiFinex ws [%s] [%s] failed to connect and subscribe to market data [retry count %d], err: %s", proxy, markets, retryCount, err)
		if retryCount > maxRetries {
			err = fmt.Errorf("Reached maximum connect retry count of %d, err: %s", maxRetries, err)
			return err
		}
		time.Sleep(3 * time.Second)
		return g.connectWs(markets, proxy, maxRetries, retryCount+1)
	}

	onDisconnect := make(chan error)
	ws.SetOnDisconnect(onDisconnect)

	retryCount = 0
	go func() {
		err := <-onDisconnect
		if g.options.Verbose {
			log.Printf("DigiFinex market data gateway ws disconnected, err: %s", err)
			log.Printf("DigiFinex reconnecting in 5 seconds...")
		}
		close(ws.Message())
		time.Sleep(3 * time.Second)
		err = g.connectWs(markets, proxy, maxRetries, retryCount)
		if err != nil {
			err = fmt.Errorf("DigiFinex failed to reconnect after disconnect, err: %s", err)
			panic(err)
		}
	}()

	go func() {
		for msg := range ws.Message() {
			err := g.processWebsocketMessage(msg)
			if err != nil {
				log.Printf("DigiFinex failed to process websocket message, error: %v, msg: %+v", err, msg)
				log.Printf("DigiFinex ws [%s] [%s] reconnecting...", proxy, markets)
				ws.Close()
				err = g.connectWs(markets, proxy, maxRetries, 0)
				if err != nil {
					err = fmt.Errorf("DigiFinex [%s] [%s] failed to reconnect, err: %s", proxy, markets, err)
					panic(err)
				}
			}
		}
	}()

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

const REQUEST_CONFIRMATION_WAIT_MODE = true

func (g *MarketDataGateway) subscribeDepth(ws *WsSession, markets []gateway.Market) error {
	symbols := make([]string, len(markets))
	for i, mkt := range markets {
		symbols[i] = mkt.Symbol
	}

	subReq := WsRequest{
		Method: "depth.subscribe",
		Params: symbols,
	}

	if REQUEST_CONFIRMATION_WAIT_MODE {
		res, err := ws.SendRequest(&subReq)
		if err != nil {
			return err
		}

		if res.Error != "" {
			return fmt.Errorf("Res err: %s", res.Error)
		}

		result := string(res.Result)
		if !strings.Contains(result, "success") {
			return fmt.Errorf("DigiFinex book depth [%s] subscribe failed, unsuccessful status, res: %s", symbols, result)
		}
	} else {
		err := ws.SendMessage(&subReq)
		if err != nil {
			return fmt.Errorf("DigiFinex failed send depth sub message: err: %s", err)
		}
	}

	return nil
}

func (g *MarketDataGateway) subscribeDeals(ws *WsSession, markets []gateway.Market) error {
	symbols := make([]string, len(markets))
	for i, mkt := range markets {
		symbols[i] = mkt.Symbol
	}

	subReq := WsRequest{
		Method: "trades.subscribe",
		Params: symbols,
	}

	if REQUEST_CONFIRMATION_WAIT_MODE {
		res, err := ws.SendRequest(&subReq)
		if err != nil {
			return err
		}

		if res.Error != "" {
			return fmt.Errorf("Res err: %s", res.Error)
		}

		result := string(res.Result)
		if !strings.Contains(result, "success") {
			return fmt.Errorf("DigiFinex deals %s subscribe failed, unsuccessful status, res: %s", symbols, result)
		}
	} else {
		err := ws.SendMessage(&subReq)
		if err != nil {
			return fmt.Errorf("DigiFinex failed send deals sub message: err: %s", err)
		}
	}

	return nil
}

func (g *MarketDataGateway) processWebsocketMessage(message WsMessage) error {
	switch message.Method {
	case "depth.update":
		err := g.processDepthUpdate(message)
		if err != nil {
			log.Println("Failed to process depth update, error:", err)
			return err
		}
	case "trades.update":
		err := g.processTrades(message)
		if err != nil {
			log.Println("Failed to process deals update, error:", err)
			return err
		}
	}

	return nil
}

func (g *MarketDataGateway) processDepthUpdate(wsMessage WsMessage) error {
	var params []json.RawMessage
	err := json.Unmarshal(wsMessage.Params, &params)
	if err != nil {
		return err
	}

	if len(params) < 3 {
		log.Println("Depth update params had less than 3 items", string(wsMessage.Params))
		return errors.New("Expected depth update params array to have at least 3 items")
	}

	var snapshotData interface{}
	var update WsDepthUpdate
	var symbol string

	err = json.Unmarshal(params[0], &snapshotData)
	if err != nil {
		return err
	}

	err = json.Unmarshal(params[1], &update)
	if err != nil {
		return err
	}

	err = json.Unmarshal(params[2], &symbol)
	if err != nil {
		return err
	}

	events := make([]gateway.Event, 0, len(update.Bids)+len(update.Asks)+1)

	// Corse snapshot value
	// Sometimes it comes as a string, sometimes its a bool
	var snapshot bool
	switch v := snapshotData.(type) {
	case string:
		snapshot = v == "true"
	case bool:
		snapshot = v
	}

	// Symbol comes as uppercase for some reason, but we
	// expect it as downcase
	symbol = strings.ToLower(symbol)

	if snapshot {
		events = append(events, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: symbol,
			},
		})
	}

	for _, price := range update.Bids {
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
	for _, price := range update.Asks {
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

func (g *MarketDataGateway) processTrades(wsMessage WsMessage) error {
	var params []json.RawMessage
	err := json.Unmarshal(wsMessage.Params, &params)
	if err != nil {
		return err
	}

	if len(params) < 3 {
		log.Println("Deals update params had less than 3 items", string(wsMessage.Params))
		err = errors.New("Expected trades update params array to have at least 3 items")
		return err
	}

	var cleanUpdate interface{}
	var deals []WsDeal
	var symbol string

	err = json.Unmarshal(params[0], &cleanUpdate)
	if err != nil {
		return err
	}
	err = json.Unmarshal(params[1], &deals)
	if err != nil {
		return err
	}
	err = json.Unmarshal(params[2], &symbol)
	if err != nil {
		return err
	}

	// Corse snapshot value
	// Sometimes it comes as a string, sometimes its a bool
	var snapshot bool
	switch v := cleanUpdate.(type) {
	case string:
		snapshot = v == "true"
	case bool:
		snapshot = v
	}

	// Symbol comes as uppercase for some reason, but we
	// expect it as downcase
	symbol = strings.ToLower(symbol)

	if !snapshot {
		events := make([]gateway.Event, len(deals))
		for i, deal := range deals {
			var direction gateway.Side
			if deal.Type == "buy" {
				direction = gateway.Bid
			} else {
				direction = gateway.Ask
			}

			events[i] = gateway.Event{
				Type: gateway.TradeEvent,
				Data: gateway.Trade{
					ID:        strconv.FormatInt(deal.Id, 10),
					Symbol:    symbol,
					Direction: direction,
					Price:     deal.Price,
					Amount:    deal.Amount,
				},
			}
		}

		g.tickCh <- gateway.Tick{
			ReceivedTimestamp: time.Now(),
			EventLog:          events,
		}
	}

	return nil
}
