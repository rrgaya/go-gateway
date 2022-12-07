package novadax

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "NovaDAX",
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
		tickCh:  make(chan gateway.Tick, 10000),
	}
}

func (gtw *Gateway) Connect() error {
	var err error
	if gtw.marketsInitialized == false {
		err = gtw.loadMarkets()
		if err != nil {
			return fmt.Errorf("Failed to load %s markets, err %s", Exchange.Name, err)
		}
	}

	gtw.accountGateway = NewAccountGateway(gtw.api, gtw.tickCh, gtw.marketsMap())
	gtw.marketDataGateway = NewMarketDataGateway(gtw.options, gtw.tickCh, gtw.api)

	err = gtw.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	if gtw.options.ApiKey != "" {
		err = gtw.accountGateway.Connect()
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
		err := gtw.loadMarkets()
		if err != nil {
			fmt.Println(err)
		}
	}

	return gtw.markets
}

func (gtw *Gateway) loadMarkets() error {
	symbols, err := gtw.api.GetSymbols()
	if err != nil {
		return err
	}

	gtw.markets = make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		gtw.markets = append(gtw.markets, symbolToCommonMarket(symbol))
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

func symbolToCommonMarket(symbol ApiSymbolData) gateway.Market {
	priceTick := 1 / math.Pow10(int(symbol.PricePrecision))
	amountTick := 1 / math.Pow10(int(symbol.AmountPrecision))
	pair := gateway.Pair{
		Base:  strings.ToUpper(symbol.BaseCurrency),
		Quote: strings.ToUpper(symbol.QuoteCurrency),
	}
	fee := 0.25 / 100

	return gateway.Market{
		Exchange:               Exchange,
		Pair:                   pair,
		Symbol:                 symbol.Symbol,
		TakerFee:               fee,
		MakerFee:               fee,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       symbol.MinOrderAmount,
		MinimumOrderMoneyValue: symbol.MinimumOrderValue,
		PaysFeeInStock:         true, // TODO: Check, I'm not sure about the value here
	}
}

func (gtw *Gateway) marketsMap() map[string]gateway.Market {
	mktMap := make(map[string]gateway.Market)

	for _, market := range gtw.markets {
		mktMap[market.Symbol] = market
	}

	return mktMap
}
