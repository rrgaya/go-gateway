package chiliz

import (
	"encoding/json"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options    gateway.Options
	marketsMap map[string]gateway.Market
	tickCh     chan gateway.Tick
	ws         *WsSession
}

func NewMarketDataGateway(opts gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options:    opts,
		marketsMap: make(map[string]gateway.Market),
		tickCh:     tickCh,
	}
}

func (mktGtw *MarketDataGateway) Connect(ws *WsSession) error {
	mktGtw.ws = ws
	return nil
}

func (mktGtw *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	mktGtw.marketsMap = make(map[string]gateway.Market)

	for _, market := range markets {
		mktGtw.marketsMap[market.Symbol] = market
	}

	log.Printf("Chiliz subscribing to %d markets...", len(markets))

	ch := make(chan WsResponse)
	mktGtw.ws.SubscribeMessages(ch, nil)
	go mktGtw.subscriptionMessageHandler(ch)

	var requests []WsRequest

	for _, market := range mktGtw.marketsMap {
		params := map[string]bool{
			"binary": false,
		}

		orderBookRequest := WsRequest{
			Symbol: market.Symbol,
			Topic:  "diffDepth",
			Event:  "sub",
			Params: params,
		}

		tradesRequest := WsRequest{
			Symbol: market.Symbol,
			Topic:  "trade",
			Event:  "sub",
			Params: params,
		}

		requests = append(requests, orderBookRequest, tradesRequest)
	}

	err := mktGtw.ws.RequestSubscriptions(requests)
	if err != nil {
		return err
	}

	return nil
}

func (mktGtw *MarketDataGateway) subscriptionMessageHandler(ch chan WsResponse) {
	for msg := range ch {
		switch msg.Topic {
		case "diffDepth":
			mktGtw.processOrderBook(msg)
		case "trade":
			mktGtw.processTrades(msg)
		}
	}
}

func (mktGtw *MarketDataGateway) processOrderBook(msg WsResponse) error {
	var eventLogs []gateway.Event

	appendOrderEvents := func(symbol string, side gateway.Side, prices []gateway.PriceArray) {
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

			eventLogs = append(eventLogs, event)
		}
	}

	if msg.FirstEntry {
		eventLogs = append(eventLogs, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: msg.Symbol,
			},
		})
	}

	diffDepthData := []WsDiffDepthDataResponse{}

	if err := json.Unmarshal(msg.Data, &diffDepthData); err != nil {
		return err
	}

	for _, data := range diffDepthData {
		appendOrderEvents(msg.Symbol, gateway.Bid, data.Bids)
		appendOrderEvents(msg.Symbol, gateway.Ask, data.Asks)
	}

	mktGtw.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}

func (mktGtw *MarketDataGateway) processTrades(msg WsResponse) error {
	if msg.FirstEntry {
		return nil
	}

	var eventLogs []gateway.Event

	tradeSideSelect := func(buying bool) gateway.Side {
		if buying {
			return gateway.Bid
		} else {
			return gateway.Ask
		}
	}

	tradeData := []WsTradeDataResponse{}

	if err := json.Unmarshal(msg.Data, &tradeData); err != nil {
		return err
	}

	for i, data := range tradeData {
		eventLogs[i] = gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Timestamp: time.Unix(0, data.Timestamp*int64(time.Millisecond)),
				Symbol:    msg.Symbol,
				ID:        data.ID,
				Direction: tradeSideSelect(data.Buying),
				Amount:    data.Amount,
				Price:     data.Price,
			},
		}
	}

	mktGtw.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLogs,
	}

	return nil
}
