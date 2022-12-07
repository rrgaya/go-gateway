package mercadobitcoin

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options              gateway.Options
	tickCh               chan gateway.Tick
	digitalAssetsMarkets map[string]bool
	fromTime             time.Time
	tapi                 *TAPI
	api                  *API
}

func NewMarketDataGateway(options gateway.Options, tickCh chan gateway.Tick, digitalAssetsMarkets map[string]bool) *MarketDataGateway {
	return &MarketDataGateway{
		options:              options,
		tickCh:               tickCh,
		digitalAssetsMarkets: digitalAssetsMarkets,
		fromTime:             time.Now(),
		tapi:                 NewTAPI(options),
		api:                  NewAPI(options),
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	ws := NewWsSession(g.options)
	err := ws.Connect()
	if err != nil {
		return err
	}

	for _, market := range markets {
		digitalAsset := g.digitalAssetsMarkets[market.Symbol]

		if digitalAsset || g.options.PollMarketData {
			go g.pollMarketDataFor(market)
		} else {
			if err := ws.WriteMessage([]byte(fmt.Sprintf("{\"type\":\"subscribe\",\"subscription\":{\"name\":\"orderbook\",\"limit\":200,\"id\":\"%s\"}}", market.Symbol))); err != nil {
				return fmt.Errorf("failed write orderbook sub msg to ws: %s", err)

			}
			if err := ws.WriteMessage([]byte(fmt.Sprintf("{\"type\":\"subscribe\",\"subscription\":{\"name\":\"trade\",\"id\":\"%s\"}}", market.Symbol))); err != nil {
				return fmt.Errorf("failed write trade sub msg to ws: %s", err)
			}
		}
	}

	ch := make(chan WsGenericMessage, 100)
	ws.SubscribeMessages(ch, nil)

	go g.messageHandler(ch)

	return nil
}

func (g *MarketDataGateway) pollMarketDataFor(market gateway.Market) {
	var refreshInterval time.Duration
	if g.options.RefreshIntervalMs > 0 {
		refreshInterval = time.Duration(g.options.RefreshIntervalMs * int(time.Millisecond))
	} else {
		refreshInterval = 2500 * time.Millisecond
	}

	log.Printf("MercadoBitcoin polling for market data of [%s] every [%s] instead of using websocket...", market.Symbol, refreshInterval)

	seq := int64(1)
	go func(market gateway.Market) {
		for {
			err := g.updateBooksFromAPI(atomic.LoadInt64(&seq), market)
			//err := g.updateBooksFromTAPI(seq, market)
			if err != nil {
				log.Printf("MercadoBitcoin market data failed to fetch order book %s, err: %s", market.Symbol, err)
				time.Sleep(5 * time.Second)
				continue
			}

			atomic.AddInt64(&seq, 1)
			time.Sleep(refreshInterval)
		}
	}(market)
	go func(market gateway.Market) {
		sinceTID := int64(0)

		for {
			lastTID, err := g.updateTradesFromAPI(atomic.LoadInt64(&seq), sinceTID, market)
			if err != nil {
				log.Printf("MercadoBitcoin market data failed to fetch trades %s, err: %s", market.Symbol, err)
				time.Sleep(5 * time.Second)
				continue
			}

			sinceTID = lastTID
			atomic.AddInt64(&seq, 1)

			// Limit refresh interval for trades
			// because we don't want to get our IP banned
			tradesRefreshInterval := refreshInterval
			if tradesRefreshInterval.Seconds() < 10 {
				tradesRefreshInterval = 10 * time.Second
			}
			time.Sleep(tradesRefreshInterval)
		}
	}(market)

	return
}

func (g *MarketDataGateway) updateTradesFromAPI(seq, sinceTID int64, market gateway.Market) (lastTID int64, err error) {
	lastTID = sinceTID

	trades, err := g.api.Trades(market.Pair.Base, sinceTID, g.fromTime)
	if err != nil {
		return
	}

	// Trades in asending order
	sort.Slice(trades, func(i, j int) bool {
		return trades[i].TID < trades[j].TID
	})

	if len(trades) > 0 {
		lastTID = trades[len(trades)-1].TID
	}

	g.processAPITradesUpdate(seq, market, trades)

	return lastTID, nil
}

func (g *MarketDataGateway) processAPITradesUpdate(seq int64, market gateway.Market, trades []APITrade) {
	events := make([]gateway.Event, 0, len(trades))

	for _, t := range trades {
		var side gateway.Side
		if t.Type == "buy" {
			side = gateway.Bid
		} else {
			side = gateway.Ask
		}

		price, _ := t.Price.Float64()
		amount, _ := t.Amount.Float64()

		events = append(events, gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Timestamp: time.Unix(t.Date, 0),
				Symbol:    market.Symbol,
				ID:        strconv.FormatInt(t.TID, 10),
				Direction: side,
				Price:     price,
				Amount:    amount,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		Sequence:          seq,
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (g *MarketDataGateway) updateBooksFromTAPI(seq int64, market gateway.Market) error {
	res, err := g.tapi.ListOrderbook(market.Symbol, true)
	if err != nil {
		return err
	}

	g.processTAPIOrderBookUpdate(seq, market, res)
	return nil
}

func (g *MarketDataGateway) updateBooksFromAPI(seq int64, market gateway.Market) error {
	res, err := g.api.OrderBook(market.Pair.Base)
	if err != nil {
		return err
	}

	g.processAPIOrderBookUpdate(seq, market, res)
	return nil
}

func (g *MarketDataGateway) apiBookRowToGtwEvent(side gateway.Side, symbol string, px APIBookRow) gateway.Event {
	price, err := px[0].Float64()
	if err != nil {
		log.Printf("failed to process price float [%s] [%s] [%s] into float64, err: %s", side, symbol, px[0], err)
	}
	amount, err := px[1].Float64()
	if err != nil {
		log.Printf("failed to process amount float [%s] [%s] [%s] into float64, err: %s", side, symbol, px[1], err)
	}

	return gateway.Event{
		Type: gateway.DepthEvent,
		Data: gateway.Depth{
			Symbol: symbol,
			Side:   side,
			Price:  price,
			Amount: amount,
		},
	}
}

func (g *MarketDataGateway) processAPIOrderBookUpdate(seq int64, market gateway.Market, book APIOrderBook) {
	events := make([]gateway.Event, 0, len(book.Bids)+len(book.Asks)+1)
	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	for _, p := range book.Bids {
		events = append(events, g.apiBookRowToGtwEvent(gateway.Bid, market.Symbol, p))
	}
	for _, p := range book.Asks {
		events = append(events, g.apiBookRowToGtwEvent(gateway.Ask, market.Symbol, p))
	}

	g.tickCh <- gateway.Tick{
		Sequence:          seq,
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (g *MarketDataGateway) processTAPIOrderBookUpdate(seq int64, market gateway.Market, book TAPIOrderbook) {
	// Merge orders into price levels
	bidsMap := make(map[float64]gateway.PriceLevel)
	asksMap := make(map[float64]gateway.PriceLevel)

	for _, o := range book.Bids {
		current, _ := bidsMap[o.LimitPrice]
		current.Price = o.LimitPrice
		current.Amount += o.Quantity
		bidsMap[o.LimitPrice] = current
	}
	for _, o := range book.Asks {
		current, _ := asksMap[o.LimitPrice]
		current.Price = o.LimitPrice
		current.Amount += o.Quantity
		asksMap[o.LimitPrice] = current
	}

	events := make([]gateway.Event, 0, len(bidsMap)+len(asksMap)+1)
	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	for _, p := range bidsMap {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   gateway.Bid,
				Price:  p.Price,
				Amount: p.Amount,
			},
		})
	}
	for _, p := range asksMap {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   gateway.Ask,
				Price:  p.Price,
				Amount: p.Amount,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		Sequence:          seq,
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (g *MarketDataGateway) messageHandler(ch chan WsGenericMessage) {
	for msg := range ch {
		switch msg.Type {
		case "orderbook":
			if err := g.processDepthUpdate(msg); err != nil {
				log.Printf("%s error processing \"%+v : data: %s\": %s", Exchange.Name, msg, string(msg.Data), err)
			}
		case "trade":
			if err := g.processTradeUpdate(msg); err != nil {
				log.Printf("%s error processing \"%+v : data: %s\": %s", Exchange.Name, msg, string(msg.Data), err)
			}
		}
	}
}

func (g *MarketDataGateway) processTradeUpdate(msg WsGenericMessage) error {
	symbol := msg.ID

	var trade WsTrade
	err := json.Unmarshal(msg.Data, &trade)
	if err != nil {
		return err
	}

	events := make([]gateway.Event, 0)

	var side gateway.Side
	if trade.Type == "buy" {
		side = gateway.Bid
	} else {
		side = gateway.Ask
	}

	events = append(events, gateway.Event{
		Type: gateway.TradeEvent,
		Data: gateway.Trade{
			Timestamp: time.Unix(trade.Date, 0),
			Symbol:    symbol,
			ID:        strconv.FormatInt(trade.TID, 10),
			Direction: side,
			Price:     trade.Price,
			Amount:    trade.Amount,
		},
	})

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	return nil
}

func (g *MarketDataGateway) processDepthUpdate(msg WsGenericMessage) error {
	symbol := msg.ID

	var orderbook WsDepth
	err := json.Unmarshal(msg.Data, &orderbook)
	if err != nil {
		return err
	}

	eventLog := make([]gateway.Event, 0, len(orderbook.Asks)+len(orderbook.Bids)+1)

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
	appendEvents(symbol, gateway.Bid, orderbook.Bids)

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}

	return nil
}
