package foxbit

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Foxbit",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            map[int]gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	tickCh             chan gateway.Tick
	ws                 *WsSession
	api                *API
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
		markets: make(map[int]gateway.Market),
		api:     NewAPI(options),
	}
}

func (g *Gateway) Connect() error {
	ws := NewWsSession(g.options)
	g.ws = ws
	err := ws.Connect()
	if err != nil {
		return err
	}

	err = g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load %s markets, err %s", Exchange.Name, err)
	}

	g.accountGateway = NewAccountGateway(ws, g.tickCh, g.api)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect()
		if err != nil {
			return err
		}
	}

	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)
	err = g.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	marketsMap := make(map[int]gateway.Market)

	for _, market := range markets {
		for instrumentId, loadedMarket := range g.markets {
			if market.Symbol == loadedMarket.Symbol {
				marketsMap[instrumentId] = loadedMarket
			}
		}
	}

	return g.marketDataGateway.SubscribeMarkets(marketsMap)
}

func (g *Gateway) Close() error {
	return nil
}

func (g *Gateway) Exchange() gateway.Exchange {
	return Exchange
}

func (g *Gateway) Markets() []gateway.Market {
	if g.marketsInitialized == false {
		_ = g.loadMarkets()
	}

	markets := make([]gateway.Market, 0, len(g.markets))

	for _, market := range g.markets {
		markets = append(markets, market)
	}

	return markets
}

func (g *Gateway) loadMarkets() error {
	functionName := "GetInstruments"
	respCh := make(chan WsMessage)
	g.ws.SubscribeMessages(functionName, respCh, nil)
	wg := sync.WaitGroup{}

	wg.Add(1)

	go func() {
		// We receive just one message here, that's why we close
		// the channel at the end of this `for`.
		for msg := range respCh {
			switch {
			case msg.Type == 0: // 0 == the response for our request
				instruments := []WsInstrument{}
				err := json.Unmarshal([]byte(msg.Payload), &instruments)
				if err != nil {
					err := fmt.Sprintf("%s error on unmarshal markets. Err: %s", Exchange.Name, err)
					panic(err)
				}

				// Instruments are what we call "markets" in the context of our application.
				// Each instrument is related to a symbol (e.g., "BRL/BTC"), and we use a map
				// to represent our markets to link the market itself with the instrument ID,
				// which is an information we need to know in order to perform other operations
				// (e.g., subscribe to the ws orderbook channel).
				for _, instrument := range instruments {
					if instrument.SessionStatus == "Running" {
						g.markets[instrument.InstrumentId] = parseToCommonMarket(instrument)
					}
				}

				g.marketsInitialized = true
			case msg.Type == 5: // 5 == ws error response
				log.Printf("%s ws failed to fetch markets. Err: %s\n", Exchange.Name, msg.Payload)
			}

			close(respCh)
			g.ws.removeSubscriber(respCh, functionName)
			wg.Done()
		}
	}()

	request := WsMessage{
		Type:           0,
		SequenceNumber: 0,
		FunctionName:   functionName,
		Payload:        `{"OMSId": 1}`,
	}

	g.ws.SendRequest(request)
	wg.Wait()

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func parseToCommonMarket(instrument WsInstrument) gateway.Market {
	pair := gateway.Pair{
		Base:  strings.ToUpper(instrument.Product1Symbol),
		Quote: strings.ToUpper(instrument.Product2Symbol),
	}

	isBrlPair := pair.Base == "BRL" || pair.Quote == "BRL"

	var fee float64
	if isBrlPair {
		fee = 0.005
	} else {
		fee = 0.0015
	}

	return gateway.Market{
		Exchange: Exchange,
		Pair:     pair,
		Symbol:   instrument.Symbol,
		TakerFee: fee,
		MakerFee: fee,
		//PriceTick:              instrument.PriceIncrement,
		PriceTick:              0.0001, // Hardcode this for now, they are returning a wrong price tick
		AmountTick:             instrument.QuantityIncrement,
		MinimumOrderSize:       instrument.MinimumQuantity,
		MinimumOrderMoneyValue: instrument.MinimumPrice,
		PaysFeeInStock:         true, // TODO: Check, I'm not sure about the value here
	}
}

/* TODO: We can use this code, once we migrate away from the current ws api
func (g *Gateway) Markets() []gateway.Market {
	if g.marketsInitialized == false {
		_ = g.loadMarkets()
	}

	return g.markets
}

var defaultAmountTick = 0.00000001
var defaultPriceTick = 0.0001

func (g *Gateway) loadMarkets() error {
	res, err := g.api.Markets()
	if err != nil {
		return fmt.Errorf("failed to load pairs: %s", err)
	}

	markets := make([]gateway.Market, len(res))
	for i, apiMarket := range res {
		priceTick := defaultPriceTick
		amountTick := defaultAmountTick

		if apiMarket.PriceIncrement != 0.0 {
			priceTick = apiMarket.PriceIncrement
		}
		if apiMarket.QuantityIncrement != 0.0 {
			amountTick = apiMarket.QuantityIncrement
		}

		base := strings.ToUpper(apiMarket.Base.Symbol)
		quote := strings.ToUpper(apiMarket.Quote.Symbol)
		symbol := fmt.Sprintf("%s/%s", base, quote)

		markets[i] = gateway.Market{
			Exchange:         Exchange,
			Pair:             gateway.Pair{Base: base, Quote: quote},
			Symbol:           symbol,
			PriceTick:        priceTick,
			AmountTick:       amountTick,
			MakerFee:         0.0025,
			TakerFee:         0.0050,
			MinimumOrderSize: apiMarket.QuantityMin,
		}
	}

	g.marketsInitialized = true
	g.markets = markets

	return nil
}
*/
