package ftx

import (
	"fmt"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "FTX",
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
	if g.marketsInitialized == false {
		err = g.loadMarkets()
		if err != nil {
			return fmt.Errorf("Failed to load FTX markets, err %s", err)
		}
	}

	ws := NewWsSession(g.options)
	err = ws.Connect()
	if err != nil {
		return err
	}

	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)
	err = g.marketDataGateway.Connect(ws)
	if err != nil {
		return err
	}

	g.accountGateway = NewAccountGateway(g.api, g.tickCh, g.options)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect()
		if err != nil {
			return err
		}
	}

	return nil
}

func (gtw *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	return gtw.marketDataGateway.SubscribeMarkets(markets)
}

func (gtw *Gateway) Close() error {
	return nil
}

func (gtw *Gateway) Exchange() gateway.Exchange {
	return Exchange
}

func (gtw *Gateway) Markets() []gateway.Market {
	if gtw.marketsInitialized == false {
		_ = gtw.loadMarkets()
	}

	return gtw.markets
}

func (gtw *Gateway) loadMarkets() error {
	markets, err := gtw.api.FetchMarkets()
	if err != nil {
		return err
	}

	gtw.marketsInitialized = true
	gtw.markets = make([]gateway.Market, 0, len(markets))

	for _, market := range markets {
		if !market.Enabled {
			continue
		}

		gtw.markets = append(gtw.markets, normalizeMarket(market))
	}

	return nil
}

func (gtw *Gateway) AccountGateway() gateway.AccountGateway {
	return nil
}

func (gtw *Gateway) Tick() chan gateway.Tick {
	return gtw.tickCh
}

func normalizeMarket(market ApiMarket) gateway.Market {
	pair := gateway.Pair{
		Base:  strings.ToUpper(market.BaseCurrency),
		Quote: strings.ToUpper(market.QuoteCurrency),
	}

	// TODO: Fees structure complexity is above what we can do for now.
	// https://help.ftx.com/hc/pt-br/articles/360024479432-Taxas
	return gateway.Market{
		Exchange:               Exchange,
		Pair:                   pair,
		Symbol:                 market.Name,
		TakerFee:               0.0007,
		MakerFee:               0.0002,
		PriceTick:              market.PriceIncrement,
		AmountTick:             market.SizeIncrement,
		MinimumOrderSize:       market.MinProvideSize,
		MinimumOrderMoneyValue: 0,    // TODO: We don't get this info from the API. No limit?
		PaysFeeInStock:         true, // TODO: Check, I'm not sure about the value here
	}
}
