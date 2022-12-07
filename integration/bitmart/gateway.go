package bitmart

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "BitMart",
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
	var err error
	if !g.marketsInitialized {
		err = g.loadMarkets()
		if err != nil {
			return fmt.Errorf("Failed to load %s markets, err: %s", Exchange.Name, err)
		}
	}

	g.accountGateway = NewAccountGateway(g.tickCh, g.api, g.options)
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

func (gtw *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	err := gtw.marketDataGateway.SubscribeMarkets(markets)
	if err != nil {
		return err
	}

	if gtw.options.ApiKey != "" {
		return gtw.accountGateway.SubscribeMarkets(markets)
	}

	return nil
}

func (gtw *Gateway) Close() error {
	return nil
}

func (gtw *Gateway) Exchange() gateway.Exchange {
	return Exchange
}

func (gtw *Gateway) Markets() []gateway.Market {
	if !gtw.marketsInitialized {
		_ = gtw.loadMarkets()
	}

	return gtw.markets
}

func (gtw *Gateway) loadMarkets() error {
	markets, err := gtw.api.GetMarkets()
	if err != nil {
		return err
	}

	gtw.markets = make([]gateway.Market, 0, len(markets))

	for _, market := range markets {
		if market.TradeStatus != "trading" {
			continue
		}

		gtw.markets = append(gtw.markets, normalizeMarket(market))
	}

	gtw.marketsInitialized = true

	return nil
}

func (gtw *Gateway) AccountGateway() gateway.AccountGateway {
	return gtw.accountGateway
}

func (gtw *Gateway) Tick() chan gateway.Tick {
	return gtw.tickCh
}

func normalizeMarket(market ApiMarket) gateway.Market {
	pair := gateway.Pair{
		Base:  strings.ToUpper(market.BaseCurrency),
		Quote: strings.ToUpper(market.QuoteCurrency),
	}

	return gateway.Market{
		Exchange:               Exchange,
		Pair:                   pair,
		Symbol:                 market.Symbol,
		TakerFee:               0.0025,
		MakerFee:               0.0025,
		PriceTick:              1 / math.Pow10(market.PriceMaxPrecision),
		AmountTick:             market.QuoteIncrement,
		MinimumOrderSize:       market.BaseMinSize,
		MinimumOrderMoneyValue: market.MinBuyAmount,
	}
}
