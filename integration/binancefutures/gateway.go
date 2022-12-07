package binancefutures

import (
	"context"
	"fmt"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/integration/binance"
)

var Exchange = gateway.Exchange{
	Name: "BinanceFutures",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	marketDataGateway  *binance.MarketDataGateway
	tickCh             chan gateway.Tick
	lapi               *binance.API
	api                *futures.Client
}

func NewGateway(options gateway.Options) gateway.Gateway {
	api := futures.NewClient(options.ApiKey, options.ApiSecret)

	return &Gateway{
		options: options,
		api:     api,
		lapi:    binance.NewAPI(options),
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	// Init market data
	g.marketDataGateway = binance.NewMarketDataGateway(g.options, g.Markets(), g.lapi, true, g.tickCh)
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
	res, err := g.api.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets, err = symbolsToCommonMarket(res.Symbols)
	if err != nil {
		return err
	}

	return nil
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
