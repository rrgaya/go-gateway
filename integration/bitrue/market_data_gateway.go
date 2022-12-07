package bitrue

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type MarketDataGateway struct {
	options    gateway.Options
	markets    []gateway.Market
	futuresApi bool
	api        *API
	tickCh     chan gateway.Tick
}

func NewMarketDataGateway(options gateway.Options, markets []gateway.Market, api *API, futuresApi bool, tickCh chan gateway.Tick) *MarketDataGateway {
	symbolsToMarket := make(map[string]gateway.Market)

	for _, market := range markets {
		symbolsToMarket[market.Symbol] = market
	}

	return &MarketDataGateway{
		options:    options,
		markets:    markets,
		futuresApi: futuresApi,
		api:        api,
		tickCh:     tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 50
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	log.Printf("Bitrue subscribing to %d markets, will need %d websocket connections, maximum of %d markets on each websocket.", len(markets), len(batches), batchesOf)

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
	ws.SetDeflate(utils.WsDeflateGzip)

	err := ws.Connect(KLineWsUrl)
	if err != nil {
		return fmt.Errorf("ws connect err: %s", err)
	}

	for _, market := range markets {
		symbol := strings.ToLower(market.Symbol)

		if err := ws.WriteMessage([]byte(fmt.Sprintf("{\"event\":\"sub\",\"params\":{\"channel\":\"market_%s_depth_step0\",\"cb_id\":\"%s\"}}", symbol, symbol))); err != nil {
			return fmt.Errorf("failed write orderbook sub msg to ws: %s", err)

		}
	}

	ch := make(chan []byte, 100)
	ws.SubscribeMessages(ch)

	go g.messageHandler(ch)

	return nil
}

type WsGenericMessage struct {
	Channel  string          `json:"channel"`
	TS       int64           `json:"ts"`
	CBID     string          `json:"cb_id"`
	EventRep string          `json:"event_rep"`
	Status   string          `json:"status"`
	Tick     json.RawMessage `json:"tick,omitempty"`
}

type WsDepth struct {
	Buys []gateway.PriceArray `json:"buys"`
	Asks []gateway.PriceArray `json:"asks"`
}

type WsTrade struct {
	Date   int64   `json:"date"`
	TID    int64   `json:"tid"`
	Type   string  `json:"type"`
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

var orderbookRegex = regexp.MustCompile(`market_(.*)_depth_step0`)

func (g *MarketDataGateway) messageHandler(ch chan []byte) {
	for data := range ch {
		var msg WsGenericMessage
		err := json.Unmarshal(data, &msg)
		if err != nil {
			log.Printf("Failed to unmarhsal wsGenericMessage [%s] err [%s]", string(data), err)
			continue
		}

		// Check errors
		if msg.EventRep == "error" {
			log.Printf("Bitrue ws msg w/ error msg: %s", string(data))
			continue
		}

		switch {
		case orderbookRegex.MatchString(msg.Channel):
			if err := g.processDepthUpdate(msg); err != nil {
				log.Printf("%s error processing \"%+v\": %s", Exchange.Name, msg, err)
			}
		}
	}
}

func (g *MarketDataGateway) processDepthUpdate(msg WsGenericMessage) error {
	// Ignore initial msg confirming the subscription to the channel
	if msg.EventRep == "subed" {
		return nil
	}

	matches := orderbookRegex.FindStringSubmatch(msg.Channel)
	if len(matches) < 2 {
		return fmt.Errorf("unable to extract symbol from channel %s, matches %s", msg.Channel, matches)
	}

	symbol := strings.ToUpper(matches[1])

	var orderbook WsDepth
	err := json.Unmarshal(msg.Tick, &orderbook)
	if err != nil {
		return err
	}

	eventLog := make([]gateway.Event, 0, len(orderbook.Asks)+len(orderbook.Buys)+1)

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

	eventLog = append(eventLog, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: symbol,
		},
	})

	appendEvents(symbol, gateway.Ask, orderbook.Asks)
	appendEvents(symbol, gateway.Bid, orderbook.Buys)

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}

	return nil
}
