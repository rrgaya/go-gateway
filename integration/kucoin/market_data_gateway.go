package kucoin

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	tickCh  chan gateway.Tick
	api     *API
	options gateway.Options
}

func NewMarketDataGateway(tickCh chan gateway.Tick, api *API, options gateway.Options) *MarketDataGateway {
	return &MarketDataGateway{
		tickCh:  tickCh,
		api:     api,
		options: options,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 100
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	for index, market := range markets {
		group := index / batchesOf

		if batches[group] == nil {
			batches[group] = make([]gateway.Market, 0)
		}

		batches[group] = append(batches[group], market)
	}

	log.Printf("KuCoin has [%d] markets, will need to distribute market data subscriptions in %d websocket connections, maximum of [%d] subscriptions on each websocket.", len(markets), len(batches), batchesOf)

	for _, batch := range batches {
		err := g.connectWs(batch, nil, 3, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *MarketDataGateway) connectWs(markets []gateway.Market, proxy *url.URL, maxRetries, retryCount int) error {
	ws := NewWsSession(g.api)

	err := g.connectWsAndSubscribeMarketData(ws, markets)
	if err != nil {
		log.Printf("KuCoin ws [%s] [%s] failed to connect and subscribe to market data [retry count %d], err: %s", proxy, markets, retryCount, err)
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
			log.Printf("KuCoin market data gateway ws disconnected, err: %s", err)
			log.Printf("KuCoin reconnecting in 5 seconds...")
		}
		time.Sleep(3 * time.Second)
		err = g.connectWs(markets, proxy, maxRetries, retryCount)
		if err != nil {
			err = fmt.Errorf("KuCoin failed to reconnect after disconnect, err: %s", err)
			panic(err)
		}
	}()

	return nil
}

func (g *MarketDataGateway) connectWsAndSubscribeMarketData(ws *WsSession, markets []gateway.Market) error {
	log.Printf("%s subscribing to %d markets...", Exchange.Name, len(markets))

	err := ws.Connect(false) // Not private ws feed
	if err != nil {
		return err
	}

	ch := make(chan WsResponse)
	ws.SubscribeMessages(ch, nil)
	go g.subscriptionMessageHandler(ch)

	symbols := make([]string, 0)
	for _, market := range markets {
		err := g.processOrderBookSnapshot(market.Symbol)
		if err != nil {
			return err
		}

		symbols = append(symbols, market.Symbol)
	}

	req := WsRequest{
		Type:     "subscribe",
		Topic:    fmt.Sprintf("/market/level2:%s", strings.Join(symbols, ",")),
		Response: true,
	}

	err = ws.SendRequest(req)
	if err != nil {
		return err
	}

	return nil
}

var tooManyRequestsMatch = regexp.MustCompile(`Too Many Requests`)

func (mg *MarketDataGateway) processOrderBookSnapshot(symbol string) error {
	maxRetries := 3
	retries := 0

	var snapshot *ApiOrderBookResponse
	for {
		res, err := mg.api.GetOrderBook(symbol)
		if err != nil {
			if tooManyRequestsMatch.MatchString(err.Error()) {
				retries += 1
				if retries < maxRetries {
					log.Printf("KuCoin get order book snapshot via api of symbol [%s] rate limited, retrying in 1 second...", symbol)
					time.Sleep(1 * time.Second)
					continue
				}
			}

			return fmt.Errorf("after [%d] tries, getOrderBook api returned err %s", retries, err)
		}

		snapshot = res
		break
	}

	eventLogs := make([]gateway.Event, len(snapshot.Asks)+len(snapshot.Bids))

	mg.appendPriceArraysToEvents(symbol, gateway.Ask, snapshot.Asks, &eventLogs)
	mg.appendPriceArraysToEvents(symbol, gateway.Bid, snapshot.Bids, &eventLogs)

	mg.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func handlerIgnoreMsgType(_type string) bool {
	if _type == "pong" || _type == "welcome" || _type == "ack" {
		return true
	}
	return false
}

func (mg *MarketDataGateway) subscriptionMessageHandler(ch chan WsResponse) {
	for msg := range ch {
		if handlerIgnoreMsgType(msg.Type) {
			continue
		}

		switch msg.Subject {
		case "trade.l2update":
			if err := mg.processOrderBook(msg); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Subject, err.Error())
			}
		default:
			log.Printf("%s ws - Received unexpected message:\n%+v\nData:\n%s", Exchange.Name, msg, string(msg.Data))
		}
	}
}

func (mg *MarketDataGateway) processOrderBook(msg WsResponse) error {
	var eventLogs []gateway.Event

	header := WsOrderBookHeader{}
	err := json.Unmarshal(msg.Data, &header)
	if err != nil {
		return fmt.Errorf("%s - Failed to unmarshal order book. Err: %s", Exchange.Name, err)
	}

	mg.appendOrderBookEntriesToEvents(header.Symbol, gateway.Bid, header.Changes.Bids, &eventLogs)
	mg.appendOrderBookEntriesToEvents(header.Symbol, gateway.Ask, header.Changes.Asks, &eventLogs)

	mg.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func (mg *MarketDataGateway) appendOrderBookEntriesToEvents(
	symbol string,
	side gateway.Side,
	prices []OrderBookEntry,
	events *[]gateway.Event,
) {
	for _, order := range prices {
		event := gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   side,
				Price:  order.Price,
				Amount: order.Size,
			},
		}

		*events = append(*events, event)
	}
}

func (mg *MarketDataGateway) appendPriceArraysToEvents(
	symbol string,
	side gateway.Side,
	prices []gateway.PriceArray,
	events *[]gateway.Event,
) {
	for _, order := range prices {
		event := gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   side,
				Price:  order.Price,
				Amount: order.Amount,
			},
		}

		*events = append(*events, event)
	}
}
