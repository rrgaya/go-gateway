package viabtc

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
	exchange gateway.Exchange
	options  gateway.Options
	tickCh   chan gateway.Tick
	url      string
	origin   string
}

func NewMarketDataGateway(exchange gateway.Exchange, options gateway.Options, tickCh chan gateway.Tick, url, origin string) *MarketDataGateway {
	return &MarketDataGateway{
		exchange: exchange,
		options:  options,
		tickCh:   tickCh,
		url:      url,
		origin:   origin,
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	log.Printf("%s opening %d websocket connections to subscribe to %d pairs", g.exchange, len(markets), len(markets))

	if len(g.options.Proxies) > 0 {
		log.Printf("%s using %d proxy servers to distribute connections", g.exchange, len(g.options.Proxies))
	}

	proxyGroups := make(map[*url.URL][]gateway.Market)
	proxyAbortCh := make(map[*url.URL]chan bool)
	for i, market := range markets {
		var proxy *url.URL
		if len(g.options.Proxies) > 0 {
			proxy = g.options.Proxies[i%len(g.options.Proxies)]
		}

		_, ok := proxyGroups[proxy]
		if !ok {
			proxyGroups[proxy] = make([]gateway.Market, 0)
		}
		proxyGroups[proxy] = append(proxyGroups[proxy], market)
		proxyAbortCh[proxy] = make(chan bool, 1)
	}

	subCh := make(chan error)
	startAt := time.Now()
	for proxy, markets := range proxyGroups {
		go func(proxy *url.URL, markets []gateway.Market) {
			for _, market := range markets {
				maxRetries := 20
				retryCount := 0
				err := g.connectWs(market, proxy, maxRetries, retryCount)
				if err != nil {
					err = fmt.Errorf("%s ws proxy [%s] market [%s] failed to connect, err: %s", g.exchange, proxy, market, err)
					subCh <- err
					return
				}

				select {
				case <-proxyAbortCh[proxy]:
					log.Printf("%s ws proxy [%s] aborting subs...", g.exchange, proxy)
					return
				default:
					subCh <- nil
				}

				time.Sleep(1 * time.Second)
			}
		}(proxy, markets)
	}

	count := 0
	for err := range subCh {
		if err != nil {
			log.Printf("%s aborting rest of ws proxy connecttions...", g.exchange)
			for proxy, _ := range proxyGroups {
				proxyAbortCh[proxy] <- true
			}
			close(subCh)
			return err
		}

		count += 1

		if count > 0 && count%50 == 0 {
			log.Printf("%s ws opened %d/%d connections...", g.exchange, count, len(markets))
		}

		if count >= len(markets) {
			break
		}
	}

	log.Printf("%s finished opening market data ws connections in %v....", g.exchange, time.Now().Sub(startAt))

	return nil
}

func (g *MarketDataGateway) connectWs(market gateway.Market, proxy *url.URL, maxRetries, retryCount int) error {
	onDisconnect := make(chan error)
	ws := NewWsSession(g.exchange, g.options, onDisconnect)

	if proxy != nil {
		ws.SetProxy(proxy)
	}

	err := g.connectWsAndSubscribeMarketData(ws, market)
	if err != nil {
		log.Printf("%s ws [%s] [%s] failed to connect and subscribe to market data [retry count %d], err: %s", g.exchange, proxy, market, retryCount, err)
		if retryCount > maxRetries {
			err = fmt.Errorf("Reached maximum connect retry count of %d, err: %s", maxRetries, err)
			return err
		}
		time.Sleep(5 * time.Second)
		return g.connectWs(market, proxy, maxRetries, retryCount+1)
	}

	retryCount = 0
	go func() {
		err := <-onDisconnect
		if g.options.Verbose {
			log.Printf("%s market data gateway ws disconnected, err: %s", g.exchange, err)
			log.Printf("%s reconnecting in 5 seconds...", g.exchange)
		}
		close(ws.Message())
		time.Sleep(5 * time.Second)
		err = g.connectWs(market, proxy, maxRetries, retryCount)
		if err != nil {
			err = fmt.Errorf("%s failed to reconnect after disconnect, err: %s", g.exchange, err)
			panic(err)
		}
	}()

	go func() {
		receivedTradesSnapshot := false

		for msg := range ws.Message() {
			err, rts := g.processWebsocketMessage(market, msg, receivedTradesSnapshot)
			receivedTradesSnapshot = rts
			if err != nil {
				log.Printf("%s failed to process websocket message, error: %v, msg: %+v", g.exchange, err, msg)
				log.Printf("%s ws [%s] [%s] reconnecting...", g.exchange, proxy, market)
				ws.Close()
				err = g.connectWs(market, proxy, maxRetries, 0)
				if err != nil {
					err = fmt.Errorf("%s [%s] [%s] failed to reconnect, err: %s", g.exchange, proxy, market, err)
					panic(err)
				}
			}
		}
	}()

	return nil
}

func (g *MarketDataGateway) connectWsAndSubscribeMarketData(ws *WsSession, market gateway.Market) error {
	err := ws.Connect(g.url, g.origin)
	if err != nil {
		return err
	}

	err = g.subscribeDepth(ws, market)
	if err != nil {
		return err
	}

	return nil
}

const REQUEST_CONFIRMATION_WAIT_MODE = false

func (g *MarketDataGateway) subscribeDepth(ws *WsSession, market gateway.Market) error {
	subscribeParams := NewDepthSubscribeParams(market.Symbol, 100, market.PriceTick)
	subReq := WsRequest{
		Method: "depth.subscribe",
		Params: subscribeParams,
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
			return fmt.Errorf("%s book depth %s subscribe failed, unsuccessful status, res: %s", g.exchange, market.Symbol, result)
		}
	} else {
		err := ws.SendMessage(&subReq)
		if err != nil {
			return fmt.Errorf("%s failed send depth sub message: err: %s", g.exchange, err)
		}
	}

	return nil
}

func (g *MarketDataGateway) subscribeDeals(ws *WsSession, market gateway.Market) error {
	subReq := WsRequest{
		Method: "deals.subscribe",
		Params: [1]string{market.Symbol},
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
			return fmt.Errorf("%s deals %s subscribe failed, unsuccessful status, res: %s", g.exchange, market.Symbol, result)
		}
	} else {
		err := ws.SendMessage(&subReq)
		if err != nil {
			return fmt.Errorf("%s failed send deals sub message: err: %s", g.exchange, err)
		}
	}

	return nil
}

func (g *MarketDataGateway) processWebsocketMessage(market gateway.Market, message WsMessage, rts bool) (error, bool) {
	switch message.Method {
	case "depth.update":
		err := g.processDepthUpdate(market, message)
		if err != nil {
			log.Printf("%s failed to process depth update, error:", g.exchange, err)
			return err, rts
		}
	case "deals.update":
		err := g.processDeals(market, message, rts)
		if err != nil {
			log.Printf("%s Failed to process deals update, error:", g.exchange, err)
			return err, rts
		}
		rts = true
	}

	return nil, rts
}

func (g *MarketDataGateway) processDepthUpdate(market gateway.Market, wsMessage WsMessage) error {
	var params []json.RawMessage
	err := json.Unmarshal(wsMessage.Params, &params)
	if err != nil {
		return err
	}

	if len(params) < 3 {
		log.Printf("%s depth update params had less than 3 items", g.exchange, string(wsMessage.Params))
		return errors.New("Expected depth update params array to have at least 3 items")
	}

	var snapshotData interface{}
	var update DepthUpdate
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

	// We are seeing incorect updates
	if market.Symbol != symbol {
		return fmt.Errorf("Received depth.update for %s on incorrect ws market %s", symbol, market.Symbol)
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
				Symbol: market.Symbol,
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
				Symbol: market.Symbol,
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

func (g *MarketDataGateway) processDeals(market gateway.Market, wsMessage WsMessage, receivedInitialSnapshot bool) error {
	var params []json.RawMessage
	err := json.Unmarshal(wsMessage.Params, &params)
	if err != nil {
		return err
	}

	if len(params) < 2 {
		log.Println("%s deals update params had less than 2 items", g.exchange, string(wsMessage.Params))
		err = errors.New("Expected trades update params array to have at least 2 items")
		return err
	}

	var deals []Deal
	var symbol string

	err = json.Unmarshal(params[0], &symbol)
	if err != nil {
		return err
	}

	err = json.Unmarshal(params[1], &deals)
	if err != nil {
		return err
	}

	// We are seeing incorect updates
	if market.Symbol != symbol {
		return fmt.Errorf("Received deals.update for %s on incorrect ws market %s", symbol, market.Symbol)
	}

	if receivedInitialSnapshot {
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
