package hotcoin

import (
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Hotcoin",
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
		api:     NewAPI(options, apiBase),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	g.accountGateway = NewAccountGateway(g.options, g.api, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("Failed to connect to account gateway, err %s", err)
		}
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)
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

func symbolsToCommonMarket(symbols []APISymbol) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		market := symbolToCommonMarket(symbol)
		commonMarkets = append(commonMarkets, market)
	}

	return commonMarkets
}

func symbolToCommonMarket(symbol APISymbol) gateway.Market {
	parts := strings.Split(symbol.Symbol, "_")
	if len(parts) < 2 {
		log.Printf("Hotcoin market [%s] impossible to extract base/quote from symbol", symbol.Symbol)
		return gateway.Market{}
	}

	return gateway.Market{
		Exchange: Exchange,
		Symbol:   symbol.Symbol,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(parts[0]),
			Quote: strings.ToUpper(parts[1]),
		},
		PriceTick:  1 / math.Pow10(int(symbol.PricePrecision)),
		AmountTick: 1 / math.Pow10(int(symbol.AmountPrecision)),
	}
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
