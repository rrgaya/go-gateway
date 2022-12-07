package biconomy

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/integration/viabtc"
)

var Exchange = gateway.Exchange{
	Name: "Biconomy",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *viabtc.MarketDataGateway
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
	g.marketDataGateway = viabtc.NewMarketDataGateway(
		Exchange,
		g.options,
		g.tickCh,
		"wss://www.biconomy.com/ws",
		"https://www.biconomy.com",
	)

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
	res, err := g.api.ExchangeInfo()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = make([]gateway.Market, 0)

	for _, exchangeInfo := range res {
		g.markets = append(g.markets, exchangeInfoToMarket(exchangeInfo))
	}

	return nil
}

func exchangeInfoToMarket(exchangeInfo APIExchangeInfo) gateway.Market {
	priceTick := 1 / math.Pow10(int(exchangeInfo.QuoteAssetPrecision))
	amountTick := 1 / math.Pow10(int(exchangeInfo.BaseAssetPrecision))

	return gateway.Market{
		Exchange: Exchange,
		Symbol:   exchangeInfo.Symbol,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(exchangeInfo.BaseAsset),
			Quote: strings.ToUpper(exchangeInfo.QuoteAsset),
		},
		PriceTick:  priceTick,
		AmountTick: amountTick,
	}
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
