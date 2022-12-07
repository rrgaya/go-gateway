package bitforex

import (
	"fmt"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "BitForex",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	tickCh             chan gateway.Tick
	api                *Api
	webapi             *WebApi
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		api:     NewApi(options),
		webapi:  NewWebApi(options),
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	g.accountGateway = NewAccountGateway(g.api, g.webapi, g.options, g.Markets(), g.tickCh)
	isAuthenticated := g.options.ApiKey != ""
	if isAuthenticated {
		err = g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("Failed to connect to account gateway, err %s", err)
		}
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.options, g.Markets(), g.tickCh)

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
	// Avoding loading markets twice, we will get rate limited
	if g.marketsInitialized == true {
		return nil
	}

	symbols, err := g.api.Symbols()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets, err = symbolsToCommonMarket(symbols)
	if err != nil {
		return err
	}

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
