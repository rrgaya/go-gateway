package mexc

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "MEXC",
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
		api:     NewAPI(options),
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to load markets, err %s", err))
	}

	g.accountGateway = NewAccountGateway(g.options, g.api, g.tickCh)
	err = g.accountGateway.Connect()
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to connect to order entry gateway, err %s", err))
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.options, g.api, g.tickCh)

	return nil
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	err := g.marketDataGateway.SubscribeMarkets(markets)
	if err != nil {
		return err
	}

	if g.options.ApiKey != "" {
		err := g.accountGateway.SubscribeMarkets(markets)
		if err != nil {
			return err
		}
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
	exchangeInfo, err := g.api.ExchangeInfo()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = make([]gateway.Market, 0)

	for _, symbol := range exchangeInfo.Symbols {
		g.markets = append(g.markets, symbolToCommonMarket(symbol))
	}

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func symbolToCommonMarket(symbol APISymbol) gateway.Market {
	priceTick := 1 / math.Pow10(int(symbol.QuotePrecision))
	amountTick := 1 / math.Pow10(int(symbol.BaseAssetPrecision))

	return gateway.Market{
		Exchange: Exchange,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(symbol.BaseAsset),
			Quote: strings.ToUpper(symbol.QuoteAsset),
		},
		Symbol:     symbol.Symbol,
		TakerFee:   0.002,
		MakerFee:   0.002,
		PriceTick:  priceTick,
		AmountTick: amountTick,
	}
}
