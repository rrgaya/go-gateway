package coinstore

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "CoinStore",
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
		api:     NewAPI(options),
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
	g.marketDataGateway = NewMarketDataGateway(g.options, g.Markets(), g.api, g.tickCh)
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
	res, err := g.api.PublicConfig()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets, err = symbolsToCommonMarket(res.SpotSymbols.List)
	if err != nil {
		return err
	}

	return nil
}

func symbolsToCommonMarket(symbols []APISpotSymbol) ([]gateway.Market, error) {
	commonMarkets := make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		market, err := symbolToCommonMarket(symbol)
		if err != nil {
			return []gateway.Market{}, err
		}

		commonMarkets = append(commonMarkets, market)
	}

	return commonMarkets, nil
}

func symbolToCommonMarket(symbol APISpotSymbol) (gateway.Market, error) {
	return gateway.Market{
		Exchange: Exchange,
		Symbol:   symbol.Symbol,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(symbol.BaseCurrency),
			Quote: strings.ToUpper(symbol.QuoteCurrency),
		},
		TakerFee:               symbol.TakerFeeRate,
		MakerFee:               symbol.MakerFeeRate,
		PriceTick:              1 / math.Pow10(int(symbol.PricePrecision)),
		AmountTick:             1 / math.Pow10(int(symbol.AmountPrecision)),
		MinimumOrderMoneyValue: symbol.LimitMinVolume,
	}, nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
