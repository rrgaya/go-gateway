package fmfw

import (
	"fmt"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "FMFW",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	tickCh             chan gateway.Tick
	api                *API
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
		api:     NewAPI(options, apiFMFW),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	g.accountGateway = NewAccountGateway(g.options, g.api, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect("wss://api.fmfw.io/api/3/ws/trading")
		if err != nil {
			return fmt.Errorf("Failed to connect to account gateway, err %s", err)
		}
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway("wss://api.fmfw.io/api/3/ws/public", g.options, g.tickCh)
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
	res, err := g.api.Symbols()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = symbolsToCommonMarket(res)

	return nil
}

func symbolsToCommonMarket(symbols map[string]APISymbol) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(symbols))

	for sym, symbol := range symbols {
		market := symbolToCommonMarket(sym, symbol)
		commonMarkets = append(commonMarkets, market)
	}

	return commonMarkets
}

func symbolToCommonMarket(sym string, symbol APISymbol) gateway.Market {
	return gateway.Market{
		Exchange: Exchange,
		Symbol:   sym,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(symbol.BaseCurrency),
			Quote: strings.ToUpper(symbol.QuoteCurrency),
		},
		TakerFee:   symbol.TakeRate,
		MakerFee:   symbol.MakeRate,
		PriceTick:  symbol.TickSize,
		AmountTick: symbol.QuantityIncrement,
	}
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
