package bkex

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type MarketDataGateway struct {
	options gateway.Options
	tickCh  chan gateway.Tick
	api     *API
}

func NewMarketDataGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options: options,
		api:     api,
		tickCh:  tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 50
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	log.Printf("Subscribing on [%s] to %d markets, will need %d websocket connections, maximum of %d markets on each websocket.", Exchange, len(markets), len(batches), batchesOf)

	for index, market := range markets {
		group := index / batchesOf

		if batches[group] == nil {
			batches[group] = make([]gateway.Market, 0)
		}

		batches[group] = append(batches[group], market)
	}

	for _, batch := range batches {
		err := g.subscribeMarketData(batch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *MarketDataGateway) subscribeMarketData(markets []gateway.Market) error {
	ws := utils.NewWsClient()

	err := ws.Connect("wss://api.bkex.com/socket.io/?EIO=3&transport=websocket")
	if err != nil {
		return fmt.Errorf("ws connect err: %s", err)
	}

	ch := make(chan []byte, 100)
	quitHandler := make(chan bool)
	finishedSubQuotation := make(chan bool, 1)
	ws.SubscribeMessages(ch)

	symbolsList := make([]string, 0)
	for _, market := range markets {
		symbolsList = append(symbolsList, market.Symbol)
	}

	go func() {
		for {
			select {
			case data := <-ch:
				switch string(data) {
				case "40":
					ws.WriteMessage([]byte("40/quotation"))
				case "40/quotation":
					// TODO: subOrderDepth is not working, for now we will use polling
					//ws.WriteMessage([]byte(fmt.Sprintf("42/quotation,[\"subOrderDepth\",{\"symbol\": \"%s\",\"number\": 50}]", strings.Join(symbolsList, ","))))
					ws.WriteMessage([]byte(fmt.Sprintf("42/quotation,[\"quotationDealConnect\",{\"symbol\": \"%s\",\"number\": 50}]", strings.Join(symbolsList, ","))))
					finishedSubQuotation <- true
					return // quit
				}
			case <-quitHandler:
				return
			}
		}
	}()

	select {
	case <-finishedSubQuotation:
	case <-time.After(5 * time.Second):
		quitHandler <- true
		return fmt.Errorf("timed out while subbing to websocket mktd data...")
	}

	quitPinger := make(chan bool)

	go g.messageHandler(ch)
	go websocketPinger(ws, 20*time.Second, quitPinger)

	ws.OnDisconnect(quitPinger)

	// TODO: When websocket is fixed, we can stop polling
	for _, market := range markets {
		go g.pollMarketDataFor(market)
	}

	return nil
}

func (g *MarketDataGateway) pollMarketDataFor(market gateway.Market) {
	var refreshInterval time.Duration
	if g.options.RefreshIntervalMs > 0 {
		refreshInterval = time.Duration(g.options.RefreshIntervalMs * int(time.Millisecond))
	} else {
		refreshInterval = 2500 * time.Millisecond
	}

	log.Printf("BKEX polling for market data of [%s] markets every [%s] instead of using websocket...", market.Symbol, refreshInterval)

	for {
		params := make(map[string]string)
		params["symbol"] = market.Symbol
		depth, err := g.api.Depth(params)
		if err != nil {
			log.Printf("BKEX failed to poll market data for [%s] err: %s", market.Symbol, err)
			time.Sleep(refreshInterval)
			continue
		}

		g.dispatchDepth(market.Symbol, depth.Bid, depth.Ask, true)
		time.Sleep(refreshInterval)
	}
}

func websocketPinger(ws *utils.WsClient, t time.Duration, quit chan bool) {
	for {
		select {
		case <-time.After(t):
			ws.WriteMessage([]byte("2"))
		case <-quit:
			return
		}
	}
}

const quotationListDealStr = "42/quotation,[\"quotationListDeal\","
const quotationOrderDepthStr = "42/quotation,[\"quotationOrderDepth\","

func startsWith(str, startsWith string) bool {
	if len(str) < len(startsWith) {
		return false
	}

	return str[0:len(startsWith)] == startsWith
}

func stripWsMsgToData(fullData []byte, prefixStr string) []byte {
	return fullData[len(prefixStr) : len(fullData)-1]
}

func (g *MarketDataGateway) messageHandler(ch chan []byte) {
	for fullData := range ch {
		if startsWith(string(fullData), quotationListDealStr) {
			data := stripWsMsgToData(fullData, quotationListDealStr)
			if err := g.processDeals(data); err != nil {
				log.Printf("%s process deals err: %s", Exchange, err)
			}
		} else if startsWith(string(fullData), quotationOrderDepthStr) {
			data := stripWsMsgToData(fullData, quotationOrderDepthStr)
			if err := g.processDepth(data); err != nil {
				log.Printf("%s process depth err: %s", Exchange, err)
			}
		}
	}
}

type WsTrade struct {
	Direction string  `json:"direction"`
	Price     float64 `json:"price,string"`
	Symbol    string  `json:"symbol"`
	Ts        int64   `json:"ts"`
	Volume    float64 `json:"volume"`
}

func (g *MarketDataGateway) processDeals(data []byte) error {
	var trades []WsTrade
	err := json.Unmarshal(data, &trades)
	if err != nil {
		return fmt.Errorf("unmarshal err: %s", err)
	}

	eventLog := make([]gateway.Event, 0)
	for _, trade := range trades {
		var side gateway.Side
		if trade.Direction == "B" {
			side = gateway.Bid
		} else {
			side = gateway.Ask
		}

		event := gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Symbol:    trade.Symbol,
				Timestamp: time.UnixMilli(trade.Ts),
				Direction: side,
				Price:     trade.Price,
				Amount:    trade.Volume,
			},
		}

		eventLog = append(eventLog, event)

	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}

	return nil
}

type WsDepth struct {
	Symbol string               `json:"symbol"`
	Asks   []gateway.PriceArray `json:"asks"`
	Bids   []gateway.PriceArray `json:"bids"`
}

func (g *MarketDataGateway) processDepth(data []byte) error {
	var depth WsDepth
	err := json.Unmarshal(data, &depth)
	if err != nil {
		return fmt.Errorf("unmarshal err: %s", err)
	}

	g.dispatchDepth(depth.Symbol, depth.Bids, depth.Asks, true)

	return nil
}

func (g *MarketDataGateway) dispatchDepth(symbol string, bids, asks []gateway.PriceArray, snapshot bool) {
	eventLog := make([]gateway.Event, 0)
	appendEvents := func(symbol string, side gateway.Side, prices []gateway.PriceArray) {
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

			eventLog = append(eventLog, event)
		}
	}

	if snapshot {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: symbol,
			},
		})
	}

	appendEvents(symbol, gateway.Ask, asks)
	appendEvents(symbol, gateway.Bid, bids)

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}
}
