package hotbit

import (
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Hotbit",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	api                ApiClient
	tickCh             chan gateway.Tick
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		api:     NewApiClient(options),
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

	go g.apiConnectionChecker()

	return nil
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	err := g.marketDataGateway.SubscribeMarkets(markets)
	if err != nil {
		return fmt.Errorf("failed to subscribe market data, err: %s", err)
	}

	log.Printf("Requesting account orders updates...")

	err = g.accountGateway.subscribeMarketsUpdate(markets)
	if err != nil {
		return fmt.Errorf("failed to subscribe account order updates, err: %s", err)
	}

	return nil
}

func (g *Gateway) Close() error {
	return nil
}

// This will keep the http connection "hot" (keep-alive)
// And will also check if we are still logged in
func (g *Gateway) apiConnectionChecker() {
	for {
		requestAt := time.Now()
		sessionInfo, err := g.api.SessionInfo()
		if err != nil {
			panic(fmt.Errorf("Failed to connect to hotbit api, err: %s", err))
		}

		if sessionInfo.LoggedIn == false {
			panic(fmt.Errorf("Logged out from hotbit api, session info %+v", sessionInfo))
		}

		executedIn := time.Now().UnixNano() - requestAt.UnixNano()

		if g.options.Verbose {
			log.Printf("Received connection info (last login ip %s) response time %fms", sessionInfo.User.LastLoginIp, float64(executedIn)/float64(time.Millisecond))
		}

		time.Sleep(5 * time.Second)
	}
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
	markets, err := g.api.GetMarkets()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = marketsToCommonMarket(markets)

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
