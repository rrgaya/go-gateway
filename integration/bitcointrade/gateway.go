package bitcointrade

import (
	"fmt"
	"math"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "BitcoinTrade",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	marketsInitialized bool
	markets            []gateway.Market
	tickCh             chan gateway.Tick
	api                *API
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		api:     NewAPI(options),
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarketsTemp()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	if g.options.Token != "" && g.options.UserID == "" {
		return fmt.Errorf("requires --userID to be passed when --token is present")
	}

	g.accountGateway = NewAccountGateway(g.options, g.Markets(), g.api, g.tickCh)
	if g.options.Token != "" {
		err := g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("failed to connect to account gateway, err %s", err)
		}

		g.accountGateway.SetUserID(g.options.UserID)
	}

	g.marketDataGateway = NewMarketDataGateway(g.options, g.Markets(), g.api, g.tickCh)

	ws := NewWsSession()
	err = ws.Connect()
	if err != nil {
		return fmt.Errorf("failed to open ws con: %s", err)
	}

	ws.SetAccountGateway(g.accountGateway)
	ws.SetMarketDataGateway(g.marketDataGateway)

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
		_ = g.loadMarketsTemp()
	}

	return g.markets
}

func (g *Gateway) loadMarketsTemp() error {
	countryList, err := g.api.GetCountryList()
	if err != nil {
		return fmt.Errorf("failed to load country list: %s", err)
	}

	var countryPairList []APICountryPair
	if len(countryList.Countries) > 0 {
		countryPairList = countryList.Countries[0].Pairs
	}

	markets := make([]gateway.Market, 0)
	for _, pair := range countryPairList {
		priceTick := 1 / math.Pow10(int(pair.DecimalPlacesQuote))
		amountTick := 1 / math.Pow10(int(pair.DecimalPlacesBase))

		markets = append(markets, gateway.Market{
			Exchange:   Exchange,
			Pair:       gateway.Pair{Base: pair.Base, Quote: pair.Quote},
			Symbol:     pair.Code,
			MakerFee:   0.0025,
			TakerFee:   0.005,
			PriceTick:  priceTick,
			AmountTick: amountTick,
		})
	}

	g.marketsInitialized = true
	g.markets = markets

	return nil
}

// TODO: Bring this back, once their API stops throttling this endpoints
func (g *Gateway) loadMarkets() error {
	pairs, err := g.api.GetPairs()
	if err != nil {
		return fmt.Errorf("failed to load pairs: %s", err)
	}

	defaultAmountTick := 0.00000001
	countryList, err := g.api.GetCountryList()
	if err != nil {
		return fmt.Errorf("failed to load country list: %s", err)
	}

	var countryPairList []APICountryPair
	if len(countryList.Countries) > 0 {
		countryPairList = countryList.Countries[0].Pairs
	}

	markets := make([]gateway.Market, 0)
	for _, pair := range pairs {
		amountTick := defaultAmountTick
		for _, cp := range countryPairList {
			if cp.Code == pair.Symbol {
				amountTick = 1 / math.Pow10(int(cp.DecimalPlacesBase))
				break
			}
		}

		markets = append(markets, gateway.Market{
			Exchange:               Exchange,
			Pair:                   gateway.Pair{Base: pair.Base, Quote: pair.Quote},
			Symbol:                 pair.Symbol,
			MakerFee:               0.0025,
			TakerFee:               0.005,
			PriceTick:              pair.PriceTick,
			AmountTick:             amountTick,
			MinimumOrderSize:       pair.MinAmount,
			MinimumOrderMoneyValue: pair.MinValue,
		})
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
