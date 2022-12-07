package gemini

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Gemini",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	marketDataGateway  *MarketDataGateway
	tickCh             chan gateway.Tick
	api                *API
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		api:     NewAPI(options),
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	var err error
	if g.marketsInitialized == false {
		err = g.loadMarkets()
		if err != nil {
			return fmt.Errorf("Failed to load %s markets, err %s", Exchange.Name, err)
		}
	}

	g.marketDataGateway = NewMarketDataGateway(g.tickCh)
	err = g.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	return g.marketDataGateway.SubscribeMarkets(markets)
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

	return g.markets
}

func (g *Gateway) loadMarkets() error {
	symbols, err := g.api.GetSymbols()
	if err != nil {
		return err
	}

	log.Printf("Gemini fetching [%d] symbol information...", len(symbols))

	markets := make([]gateway.Market, len(symbols))
	wg := &sync.WaitGroup{}
	wg.Add(len(markets))
	for i, symbol := range symbols {
		go func(i int, symbol string) {
			defer wg.Done()
			details, err := g.api.GetSymbolDetails(symbol)

			if err != nil {
				log.Printf("Gemini failed to fetch symbol [%s] details, api returned err: %s", symbol, err)
				return
			}

			markets[i] = g.parseToCommonMarket(details)
		}(i, symbol)
	}
	wg.Wait()

	g.markets = markets
	g.marketsInitialized = true

	return nil
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func (g *Gateway) parseToCommonMarket(market *ApiSymbolDetail) gateway.Market {
	pair := gateway.Pair{
		Base:  strings.ToUpper(market.BaseCurrency),
		Quote: strings.ToUpper(market.QuoteCurrency),
	}
	priceTick, _ := market.QuoteIncrement.Float64()
	tickSize, _ := market.TickSize.Float64()

	return gateway.Market{
		Exchange:               Exchange,
		Pair:                   pair,
		Symbol:                 market.Symbol,
		TakerFee:               0.001,
		MakerFee:               0.001,
		PriceTick:              priceTick,
		AmountTick:             tickSize,
		MinimumOrderSize:       market.MinOrderSize,
		MinimumOrderMoneyValue: 0,    // TODO: We don't get this info from the API. No limit?
		PaysFeeInStock:         true, // TODO: Check, I'm not sure about the value here
	}
}
