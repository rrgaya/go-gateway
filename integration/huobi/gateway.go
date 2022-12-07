package huobi

import (
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Huobi",
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
	ws                 *WsSession
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
		return errors.New(fmt.Sprintf("Failed to load markets, err %s", err))
	}

	if g.options.ApiKey != "" {
		accounts, err := g.api.Accounts()
		if err != nil {
			panic(fmt.Errorf("Huobi failed to get accounts err: %s", err))
		}

		if len(accounts) < 1 {
			panic(fmt.Errorf("Huobi expected to have at least 1 account, %d where found", len(accounts)))
		}

		log.Printf("Returned %d accounts in huobi, %+v", len(accounts), accounts)

		accountId := accounts[0].ID
		log.Printf("Huobi using account id %d", accountId)

		g.ws = NewWsSession(1, g.options)
		err = g.ws.Connect()
		if err != nil {
			return err
		}
		err = g.ws.Authenticate()
		if err != nil {
			panic(fmt.Errorf("Failed to authenticate w/ huobi, err %s", err))
		}

		g.accountGateway = NewAccountGateway(accountId, g.api, g.ws, g.tickCh)
		err = g.accountGateway.Connect()
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to connect to order entry gateway, err %s", err))
		}

		go g.apiConnectionKeepalive()
	} else {
		g.accountGateway = NewAccountGateway(0, g.api, g.ws, g.tickCh)
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)

	return nil
}

func (g *Gateway) apiConnectionKeepalive() {
	for {
		requestAt := time.Now()
		timestamp, err := g.api.Timestamp()
		if err != nil {
			panic(fmt.Errorf("Huobi failed to get timestamp for apiConnectionKeepalive err", err))
		}
		finishedAt := time.Now()

		if g.options.Verbose {
			log.Printf("Huobi api connection pinged, response time %v, timestamp diff %dms", finishedAt.Sub(requestAt), (finishedAt.UnixNano()/int64(time.Millisecond))-timestamp)
		}

		time.Sleep(15 * time.Second)
	}
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
	symbols, err := g.api.Symbols()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		if symbol.State == "online" {
			g.markets = append(g.markets, symbolToCommonMarket(symbol))
		}
	}

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func symbolToCommonMarket(symbol APISymbol) gateway.Market {
	sym := symbol.BaseCurrency + symbol.QuoteCurrency
	priceTick := 1 / math.Pow10(int(symbol.PricePrecision))
	amountTick := 1 / math.Pow10(int(symbol.AmountPrecision))

	return gateway.Market{
		Exchange: Exchange,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(symbol.BaseCurrency),
			Quote: strings.ToUpper(symbol.QuoteCurrency),
		},
		Symbol:                 sym,
		TakerFee:               0.002,
		MakerFee:               0.002,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       symbol.MinimumOrderAmount,
		MinimumOrderMoneyValue: symbol.MinimumOrderValue,
		PaysFeeInStock:         true,
	}
}
