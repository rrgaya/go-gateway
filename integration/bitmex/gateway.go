package bitmex

import (
	"fmt"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/qct/bitmex-go/swagger"
)

var Exchange = gateway.Exchange{
	Name: "BitMEX",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	marketDataGateway  *MarketDataGateway
	accountGateway     *AccountGateway
	tickCh             chan gateway.Tick
	sdk                *swagger.APIClient
	api                *API
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		sdk:     swagger.NewAPIClient(swagger.NewConfiguration()),
		api:     NewAPI(options),
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	var err error
	if g.marketsInitialized == false {
		err = g.loadMarkets()
		if err != nil {
			return fmt.Errorf("Failed to load markets, err %s", err)
		}
	}

	ws := NewWsSession(g.options)
	err = ws.Connect()
	if err != nil {
		return err
	}

	g.accountGateway = NewAccountGateway(g.options, g.api, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect(ws)
		if err != nil {
			return fmt.Errorf("Failed to connect to account gateway, err %s", err)
		}
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)
	err = g.marketDataGateway.Connect(ws)
	if err != nil {
		return err
	}

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

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	return g.marketDataGateway.SubscribeMarkets(markets)
}

func (g *Gateway) SetMarkets(markets []gateway.Market) error {
	g.markets = markets
	g.marketsInitialized = true
	return nil
}

func (g *Gateway) loadMarkets() error {
	intruments, _, err := g.sdk.InstrumentApi.InstrumentGetActive()
	if err != nil {
		return err
	}

	markets := make([]gateway.Market, len(intruments))
	for i, intrument := range intruments {
		markets[i] = gateway.Market{
			Exchange: Exchange,
			Symbol:   intrument.Symbol,
			Pair: gateway.Pair{
				Base:  intrument.RootSymbol,
				Quote: intrument.QuoteCurrency,
			},
			MakerFee:           -0.00025,
			TakerFee:           0.00075,
			PriceTick:          intrument.TickSize,
			AmountTick:         1, // BitMEX doesn't negotiate fractional contracts
			FuturesContract:    true,
			FuturesMarginAsset: "XBT",
		}
	}

	g.marketsInitialized = true
	g.markets = markets

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
