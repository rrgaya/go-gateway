package lbank

import (
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "LBank",
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
	if g.options.ApiSecret != "" {
		err := g.api.LoadPrivateKey(g.options.ApiSecret)
		if err != nil {
			return fmt.Errorf("Failed to load api private key, err %s", err)
		}
	}

	if g.marketsInitialized == false {
		err := g.loadMarkets()
		if err != nil {
			return fmt.Errorf("Failed to load markets, err %s", err)
		}
	}

	g.accountGateway = NewAccountGateway(g.api, g.tickCh, g.options)
	if g.options.ApiKey != "" {
		err := g.accountGateway.Connect()
		if err != nil {
			return err
		}
	}

	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)
	err := g.marketDataGateway.Connect()
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
	pairs, err := g.api.TradingPairs()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = make([]gateway.Market, 0, len(pairs))

	for _, pair := range pairs {
		g.markets = append(g.markets, tradingPairtoCommum(pair))
	}

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func tradingPairtoCommum(pair APITradingPair) gateway.Market {
	priceTick := 1 / math.Pow10(int(pair.PriceAccuracy))
	amountTick := 1 / math.Pow10(int(pair.QuantityAccuracy))

	parts := strings.SplitN(pair.Symbol, "_", 2)
	if len(parts) < 2 {
		log.Printf("unable to spit pair.Symbol [%s] into base/quote", pair.Symbol)
		return gateway.Market{}
	}

	p := gateway.Pair{
		Base:  strings.ToUpper(parts[0]),
		Quote: strings.ToUpper(parts[1]),
	}

	return gateway.Market{
		Exchange:         Exchange,
		Pair:             p,
		Symbol:           pair.Symbol,
		TakerFee:         0.001,
		MakerFee:         0.001,
		PriceTick:        priceTick,
		AmountTick:       amountTick,
		MinimumOrderSize: pair.MinTranQua,
	}
}
