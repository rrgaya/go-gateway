package bigone

import (
	"fmt"
	"log"
	"math"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "BigONE",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	api                APIClient
	tickCh             chan gateway.Tick
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		api:     NewAPIClient(options),
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("load markets: %s", err)
	}

	g.accountGateway = NewAccountGateway(g.api, g.options, g.tickCh)
	err = g.accountGateway.Connect()
	if err != nil {
		return fmt.Errorf("account gtw connect: %s", err)
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.api, g.options, g.Markets(), g.tickCh)

	return nil
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	err := g.marketDataGateway.SubscribeMarkets(markets)
	if err != nil {
		return fmt.Errorf("failed to subscribe market data, err: %s", err)
	}

	if g.options.ApiKey != "" {
		log.Printf("Requesting account orders updates...")

		err = g.accountGateway.subscribeMarketsUpdate(markets)
		if err != nil {
			return fmt.Errorf("failed to subscribe account order updates, err: %s", err)
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
	pairs, err := g.api.AssetPairs()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = pairsToCommonMarket(pairs)

	return nil
}

func pairsToCommonMarket(pairs []APIAssetPair) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(pairs))

	for _, pair := range pairs {
		mkt, err := pairToCommonMarket(pair)
		if err == nil {
			commonMarkets = append(commonMarkets, mkt)
		}
	}

	return commonMarkets
}

func pairToCommonMarket(pair APIAssetPair) (mkt gateway.Market, err error) {
	priceTick := 1 / math.Pow10(int(pair.QuoteScale))
	amountTick := 1 / math.Pow10(int(pair.BaseScale))

	return gateway.Market{
		Exchange: Exchange,
		Pair: gateway.Pair{
			Base:  pair.BaseAsset.Symbol,
			Quote: pair.QuoteAsset.Symbol,
		},
		Symbol:                 pair.Name,
		TakerFee:               0.00,
		MakerFee:               0.00,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderMoneyValue: pair.MinQuoteValue,
	}, nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
