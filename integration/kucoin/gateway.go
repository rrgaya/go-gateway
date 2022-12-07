package kucoin

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "KuCoin",
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
			return fmt.Errorf("Failed to load %s markets, err %s", Exchange.Name, err)
		}
	}

	g.accountGateway = NewAccountGateway(g.api, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect()
		if err != nil {
			return err
		}
	}

	g.marketDataGateway = NewMarketDataGateway(g.tickCh, g.api, g.options)
	err = g.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (gtw *Gateway) AccountGateway() gateway.AccountGateway {
	return gtw.accountGateway
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
	if !g.marketsInitialized {
		_ = g.loadMarkets()
	}

	return g.markets
}
func (gtw *Gateway) loadMarkets() error {
	markets, err := gtw.api.GetMarkets()
	if err != nil {
		return err
	}

	gtw.markets = make([]gateway.Market, 0, len(markets))

	for _, market := range markets {
		gtw.markets = append(gtw.markets, gtw.parseToCommonMarket(&market))
	}

	gtw.marketsInitialized = true

	return nil
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func (g *Gateway) parseToCommonMarket(market *ApiMarket) gateway.Market {
	pair := gateway.Pair{
		Base:  strings.ToUpper(market.BaseCurrency),
		Quote: strings.ToUpper(market.QuoteCurrency),
	}
	priceTick, _ := strconv.ParseFloat(market.BaseIncrement, 64)
	amountTick, _ := strconv.ParseFloat(market.PriceIncrement, 64)
	minimumOrderSize, _ := strconv.ParseFloat(market.BaseMinSize, 64)
	minimumOrderMoneyValue, _ := strconv.ParseFloat(market.QuoteMinSize, 64)

	return gateway.Market{
		Exchange:               Exchange,
		Pair:                   pair,
		Symbol:                 market.Symbol,
		TakerFee:               0.001,
		MakerFee:               0.001,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       minimumOrderSize,
		MinimumOrderMoneyValue: minimumOrderMoneyValue,
		PaysFeeInStock:         true, // TODO: Check, I'm not sure about the value here
	}
}
