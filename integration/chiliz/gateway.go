package chiliz

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Chiliz",
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

// https://chiliz.zendesk.com/hc/en-us/articles/360011405940-Chiliz-net-Fee-Structure
const chzTakerFee = 0
const chzMakerFee = 0.006
const nonChzTakerFee = 0.002
const nonChzMakerFee = 0.002

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		api:     NewAPI(options),
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (gtw *Gateway) Connect() error {
	var err error
	if gtw.marketsInitialized == false {
		err = gtw.loadMarkets()
		if err != nil {
			return fmt.Errorf("Failed to load Chiliz markets, err %s", err)
		}
	}

	ws := NewWsSession(gtw.options)
	err = ws.Connect()
	if err != nil {
		return err
	}

	gtw.accountGateway = NewAccountGateway(gtw.api, gtw.tickCh, gtw.markets)
	gtw.marketDataGateway = NewMarketDataGateway(gtw.options, gtw.tickCh)

	err = gtw.marketDataGateway.Connect(ws)
	if err != nil {
		return err
	}

	err = gtw.accountGateway.Connect()
	if err != nil {
		return err
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
	symbols, err := gtw.api.Symbols()
	if err != nil {
		return err
	}

	gtw.marketsInitialized = true
	gtw.markets = make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		if symbol.Status == "TRADING" {
			gtw.markets = append(gtw.markets, symbolToCommonMarket(symbol))
		}
	}

	return nil
}

func (gtw *Gateway) AccountGateway() gateway.AccountGateway {
	return gtw.accountGateway
}

func (gtw *Gateway) Tick() chan gateway.Tick {
	return gtw.tickCh
}

func symbolToCommonMarket(symbol SymbolResponse) gateway.Market {
	sym := symbol.BaseAsset + symbol.QuoteAsset
	priceTick := 1 / math.Pow10(int(symbol.BaseAssetPrecision))
	amountTick := 1 / math.Pow10(int(symbol.QuotePrecision))

	priceFilter, err := symbol.SelectFilter("PRICE_FILTER")
	if err != nil {
		panic(err)
	}

	lotFilter, err := symbol.SelectFilter("LOT_SIZE")
	if err != nil {
		panic(err)
	}

	pair := gateway.Pair{
		Base:  strings.ToUpper(symbol.BaseAsset),
		Quote: strings.ToUpper(symbol.QuoteAsset),
	}

	return gateway.Market{
		Exchange:               Exchange,
		Pair:                   pair,
		Symbol:                 sym,
		TakerFee:               selectTakerFee(pair),
		MakerFee:               selectMakerFee(pair),
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       lotFilter.MinQty,
		MinimumOrderMoneyValue: priceFilter.MinPrice,
		PaysFeeInStock:         true, // TODO: Check, I'm not sure about the value here
	}
}

func selectTakerFee(pair gateway.Pair) float64 {
	if isChzSymbol(pair) {
		return chzTakerFee
	}

	return nonChzTakerFee
}

func selectMakerFee(pair gateway.Pair) float64 {
	if isChzSymbol(pair) {
		return chzMakerFee
	}

	return nonChzMakerFee
}

func isChzSymbol(pair gateway.Pair) bool {
	return pair.Base == "CHZ" || pair.Quote == "CHZ"
}
