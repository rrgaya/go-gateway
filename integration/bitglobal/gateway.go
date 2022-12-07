package bitglobal

import (
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "BitGlobal",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	api                *API
	tickCh             chan gateway.Tick
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
		return fmt.Errorf("load markets: %s", err)
	}

	g.accountGateway = NewAccountGateway(g.api, g.options, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("account gtw connect: %s", err)
		}
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)

	return nil
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	err := g.marketDataGateway.SubscribeMarkets(markets)
	if err != nil {
		return fmt.Errorf("failed to subscribe market data, err: %s", err)
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
	res, err := g.api.SpotConfig()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = marketsToCommonMarket(res.SpotConfig)

	return nil
}

func marketsToCommonMarket(markets []APISpotMarket) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(markets))

	for _, market := range markets {
		mkt, err := marketToCommonMarket(market)
		if err != nil {
			log.Println(err)
			continue
		}
		commonMarkets = append(commonMarkets, mkt)
	}

	return commonMarkets
}

func marketToCommonMarket(market APISpotMarket) (mkt gateway.Market, err error) {
	parts := strings.Split(market.Symbol, "-")
	if len(parts) < 2 {
		err = fmt.Errorf("BitGlobal failed to translate symbol [%s] into base/quote", market.Symbol)
		log.Println(err)
		return mkt, err
	}
	base := strings.ToUpper(parts[0])
	quote := strings.ToUpper(parts[1])

	if len(market.Accuracy) < 2 {
		err = fmt.Errorf("BitGlobal expected market accuracy array to contain at least 2 accuracies, instead [%+v]", market.Accuracy)
		return mkt, err
	}

	pricePrec, err := market.Accuracy[0].Int64()
	if err != nil {
		err = fmt.Errorf("BitGlobal failed to parse price precision to Int64 [%s] [%s]", market.Accuracy[0], err)
		return mkt, err
	}
	amountPrec, err := market.Accuracy[1].Int64()
	if err != nil {
		err = fmt.Errorf("BitGlobal failed to parse amount precision to Int64 [%s] [%s]", market.Accuracy[0], err)
		return mkt, err
	}

	priceTick := 1 / math.Pow10(int(pricePrec))
	amountTick := 1 / math.Pow10(int(amountPrec))

	return gateway.Market{
		Exchange: Exchange,
		Pair: gateway.Pair{
			Base:  base,
			Quote: quote,
		},
		Symbol:     market.Symbol,
		PriceTick:  priceTick,
		AmountTick: amountTick,
	}, nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
