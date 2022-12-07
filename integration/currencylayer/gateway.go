package currencylayer

import (
	"log"
	"math"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "CurrencyLayer",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	markets            []gateway.Market
	marketsInitialized bool
	marketDataGateway  *MarketDataGateway
	api                *API
	tickCh             chan gateway.Tick
}

func NewGateway(options gateway.Options) gateway.Gateway {
	if options.CurrencyLayerSource == "" {
		options.CurrencyLayerSource = "USD" // Use USD as the base currency for the markets by default
	}

	if options.RefreshIntervalMs == 0 {
		options.RefreshIntervalMs = 300 * 1000 // 5 minutes in milliseconds
	} else if options.RefreshIntervalMs < 1000 {
		log.Printf("CurrencyLayer minimum RefreshIntervalMs is 1000ms, cannot use %dms, defaulting to 1000ms...", options.RefreshIntervalMs)
		options.RefreshIntervalMs = 1000
	}

	api := NewAPI(options.ApiKey)

	return &Gateway{
		api:     api,
		options: options,
		tickCh:  make(chan gateway.Tick, 10),
	}
}

func (g *Gateway) Connect() error {
	g.marketDataGateway = NewMarketDataGateway(g.Markets(), g.api, g.options, g.tickCh)
	err := g.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	return nil
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
	quotes, err := g.api.GetQuotes(g.options.CurrencyLayerSource)
	if err != nil {
		log.Printf("CurrencyLayer failed to load markets while fetching quotes, err: %s", err)
		return err
	}

	g.marketsInitialized = true
	g.markets = make([]gateway.Market, len(quotes))
	for i, quote := range quotes {
		g.markets[i] = gateway.Market{
			Exchange: Exchange,
			Pair: gateway.Pair{
				Base:  quote.Base,
				Quote: quote.Quote,
			},
			Symbol:     quote.Symbol,
			PriceTick:  1 / math.Pow10(quote.Precision),
			AmountTick: 0.01, // USD price tick. TODO: What if we use another source currency
		}
	}

	return nil
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
