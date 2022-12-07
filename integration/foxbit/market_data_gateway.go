package foxbit

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

// The order book response is represented by a single array.
// The indexes bellow help us to know what each array position means.
const (
	orderInstrumentIdIdx = 7
	orderSideIdx         = 9
	orderPriceIdx        = 6
	orderAmountIdx       = 8
	tradeIdIdx           = 0
	tradeInstrumentIdIdx = 1
	tradeAmountIdx       = 2
	tradePriceIdx        = 3
	tradeSideIdx         = 8
)

type MarketDataGateway struct {
	options    gateway.Options
	marketsMap map[int]gateway.Market
	tickCh     chan gateway.Tick
}

func NewMarketDataGateway(opts gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options:    opts,
		marketsMap: make(map[int]gateway.Market),
		tickCh:     tickCh,
	}
}

func (mg *MarketDataGateway) Connect() error {
	return nil
}

func (mg *MarketDataGateway) SubscribeMarkets(markets map[int]gateway.Market) error {
	mg.marketsMap = markets

	log.Printf("%s subscribing to %d markets...", Exchange.Name, len(markets))

	orderBookFunctionName := "SubscribeLevel2"
	tradeFunctionName := "SubscribeTrades"
	respCh := make(chan WsMessage)

	go mg.subscriptionMessageHandler(respCh)

	var requests []WsMessage

	for instrumentId := range mg.marketsMap {
		ws := NewWsSession(mg.options)
		err := ws.Connect()
		if err != nil {
			return fmt.Errorf("failed to open ws for instrument [%s], err: %s", instrumentId, err)
		}

		orderBookRequest := WsMessage{
			Type:           0,
			SequenceNumber: 0,
			FunctionName:   orderBookFunctionName,
			Payload:        fmt.Sprintf(`{"OMSId": 1, "InstrumentId": %v, "Depth": 100}`, instrumentId),
		}

		tradesRequest := WsMessage{
			Type:           0,
			SequenceNumber: 0,
			FunctionName:   tradeFunctionName,
			Payload:        fmt.Sprintf(`{"OMSId": 1, "InstrumentId": %v, "Depth": 100}`, instrumentId),
		}

		requests = append(requests, orderBookRequest, tradesRequest)

		ws.SubscribeMessages(orderBookFunctionName, respCh, nil)
		ws.SubscribeMessages(tradeFunctionName, respCh, nil)
		ws.SubscribeMessages("Level2UpdateEvent", respCh, nil)
		ws.SubscribeMessages("TradeDataUpdateEvent", respCh, nil)

		err = ws.RequestSubscriptions(requests)
		if err != nil {
			return fmt.Errorf("failed to open ws for instrument [%s], err: %s", instrumentId, err)
		}
	}

	return nil
}

func (mg *MarketDataGateway) subscriptionMessageHandler(ch chan WsMessage) {
	printError := func(err error, functionName string) {
		if err != nil {
			log.Printf("%s error processing msg from channel %q, err: %s\n", Exchange.Name, functionName, err)
		}
	}

	for msg := range ch {
		// After a subscription we receive type 0 with a response.
		// The subsequent messages have type 3 representing an event
		// with another response.
		if msg.Type != 0 && msg.Type != 3 {
			continue
		}

		switch msg.FunctionName {
		// Althoug we subscribe for "SubscribeLevel2", the subsequent responses
		// are named "Level2UpdateEvent" (msg.Type == 3). The same logic applyes
		// for the "SubscribeTrades".
		case "SubscribeLevel2", "Level2UpdateEvent":
			err := mg.processOrderBook(msg)
			printError(err, msg.FunctionName)
		case "SubscribeTrades", "TradeDataUpdateEvent":
			err := mg.processTrades(msg)
			printError(err, msg.FunctionName)
		}
	}
}

func (mg *MarketDataGateway) processOrderBook(msg WsMessage) error {
	var eventLogs []gateway.Event

	var orders [][]float64

	if err := json.Unmarshal([]byte(msg.Payload), &orders); err != nil {
		log.Printf("Failed to unmarshal %s ws order book update. Err: %s. \n\nData: %s", Exchange.Name, err, msg.Payload)
		return err
	}

	if msg.SequenceNumber == 0 {
		instrumentId := int(orders[0][orderInstrumentIdIdx])

		eventLogs = append(eventLogs, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: mg.marketsMap[instrumentId].Symbol,
			},
		})
	}

	for _, order := range orders {
		instrumentId := int(order[orderInstrumentIdIdx])
		side := normalizeSide(int(order[orderSideIdx]))
		price := order[orderPriceIdx]
		amount := order[orderAmountIdx]

		event := gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: mg.marketsMap[instrumentId].Symbol,
				Side:   side,
				Price:  price,
				Amount: amount,
			},
		}

		eventLogs = append(eventLogs, event)
	}

	mg.dispatchEventLogs(eventLogs)

	return nil
}

func (mg *MarketDataGateway) processTrades(msg WsMessage) error {
	var eventLogs []gateway.Event

	var trades [][11]interface{}

	if err := json.Unmarshal([]byte(msg.Payload), &trades); err != nil {
		log.Printf("Failed to unmarshal %s ws trades. Err: %s. \n\nData: %s", Exchange.Name, err, msg.Payload)
		return err
	}

	for _, trade := range trades {
		tradeIdENotation, _ := strconv.ParseFloat(fmt.Sprint(trade[tradeIdIdx]), 64) // e.g., 6.485983e+06
		tradeId := fmt.Sprint(int(tradeIdENotation))

		instrumentId, _ := strconv.Atoi(fmt.Sprint(trade[tradeInstrumentIdIdx]))
		side, _ := strconv.Atoi(fmt.Sprint(trade[tradeSideIdx]))
		amount, _ := strconv.ParseFloat(fmt.Sprint(trade[tradeAmountIdx]), 64)
		price, _ := strconv.ParseFloat(fmt.Sprint(trade[tradePriceIdx]), 64)

		eventLogs = append(eventLogs, gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Symbol:    mg.marketsMap[instrumentId].Symbol,
				ID:        tradeId,
				Direction: normalizeSide(side),
				Amount:    amount,
				Price:     price,
			},
		})
	}

	mg.dispatchEventLogs(eventLogs)

	return nil
}

func (mg *MarketDataGateway) dispatchEventLogs(events []gateway.Event) {
	mg.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func normalizeSide(side int) gateway.Side {
	if side == 0 {
		return gateway.Bid
	}

	return gateway.Ask
}
