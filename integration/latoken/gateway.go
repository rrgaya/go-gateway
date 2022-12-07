package latoken

import (
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "LAToken",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	api                *ApiClient
	ws                 *WsSession
	tickCh             chan gateway.Tick
	currencies         []APICurrency
	symbolsMap         map[string]gateway.Market
	marketsMap         map[gateway.Market]APIPair
	markets            []gateway.Market
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
		api:     NewApiClient(options),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	// Get user detail if logged in
	var userID string
	if g.options.ApiKey != "" || g.options.Cookie != "" {
		user, err := g.api.GetUser()
		if err != nil {
			return fmt.Errorf("failed to get user detail, err: %s", err)
		}

		log.Printf("LAToken user id: %s", user.ID)

		userID = user.ID
	}

	// Channels to connect websocket w/ order entry gateway and market data
	orderUpdateCh := make(chan OrderUpdate, 5)
	depthUpdateCh := make(chan DepthUpdate, 5)

	// Connect to websocket
	g.ws = NewWsSession(g.options, userID, orderUpdateCh, depthUpdateCh, g.symbolsMap, g.marketsMap, g.currencies)
	err = g.ws.Connect()
	if err != nil {
		return err
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(depthUpdateCh, g.ws, g.tickCh)
	err = g.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	// Order entry gateway
	g.accountGateway = NewAccountGateway(g.api, orderUpdateCh, g.currencies, g.tickCh)
	err = g.accountGateway.Init()
	if err != nil {
		return err
	}

	// Check if session is still active
	// This will also prevent the session from going stale
	if g.options.ApiKey != "" || g.options.Cookie != "" {
		go func() {
			for {
				err := g.checkSessionActive()
				if err != nil {
					panic(err)
				}

				time.Sleep(15 * time.Second)
			}
		}()
	}

	return nil
}

func (g *Gateway) checkSessionActive() error {
	user, err := g.api.GetUser()
	if err != nil {
		return fmt.Errorf("checkSessionActive failed to get user detail, err: %s", err)
	}

	if user.Status != "ACTIVE" {
		return fmt.Errorf("checkSessionActive user status was not active, instead returned: %s", user.Status)
	}

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
		err := g.loadMarkets()
		if err != nil {
			log.Printf("Failed to load markets, err: %s", err)
		}
	}

	return g.markets
}

func (g *Gateway) loadMarkets() error {
	currencies, err := g.api.GetCurrencies()
	if err != nil {
		return fmt.Errorf("api get currencies err: %s", err)
	}

	pairs, err := g.api.GetPairs()
	if err != nil {
		return fmt.Errorf("api get pairs err: %s", err)
	}

	g.symbolsMap = make(map[string]gateway.Market)
	g.marketsMap = make(map[gateway.Market]APIPair)
	g.currencies = currencies

	for _, pair := range pairs {
		baseCurrency, ok := findCurrency(currencies, pair.BaseCurrency)
		if !ok {
			return fmt.Errorf("failed to locate base currency id %s", pair.BaseCurrency)
		}
		quoteCurrency, ok := findCurrency(currencies, pair.QuoteCurrency)
		if !ok {
			return fmt.Errorf("failed to locate quote currency id %s", pair.QuoteCurrency)
		}
		symbol := pairToSymbol(baseCurrency.Tag, quoteCurrency.Tag)

		market := gateway.Market{
			Exchange: Exchange,
			Pair: gateway.Pair{
				Base:  baseCurrency.Tag,
				Quote: quoteCurrency.Tag,
			},
			Symbol:           symbol,
			TakerFee:         0.001,
			MakerFee:         0.001,
			PriceTick:        pair.PriceTick,
			AmountTick:       pair.QuantityTick,
			MinimumOrderSize: pair.MinOrderQuantity,
		}

		g.symbolsMap[symbol] = market
		g.marketsMap[market] = pair
		g.markets = append(g.markets, market)
	}

	g.marketsInitialized = true

	return nil

}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
