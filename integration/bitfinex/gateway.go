package bitfinex

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	bfxbook "github.com/bitfinexcom/bitfinex-api-go/pkg/models/book"
	bfxnf "github.com/bitfinexcom/bitfinex-api-go/pkg/models/notification"
	bfxorder "github.com/bitfinexcom/bitfinex-api-go/pkg/models/order"
	bfxtrade "github.com/bitfinexcom/bitfinex-api-go/pkg/models/trade"
	bfxexec "github.com/bitfinexcom/bitfinex-api-go/pkg/models/tradeexecution"
	bfxv1 "github.com/bitfinexcom/bitfinex-api-go/v1"
	"github.com/bitfinexcom/bitfinex-api-go/v2/rest"
	"github.com/bitfinexcom/bitfinex-api-go/v2/websocket"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/op/go-logging"
)

var Exchange = gateway.Exchange{
	Name: "Bitfinex",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	api                *API
	markets            []gateway.Market
	marketsInitialized bool
	marketDataGateway  *MarketDataGateway
	accountGateway     *AccountGateway
	tickCh             chan gateway.Tick
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		api:     NewAPI(options),
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	// Quiet bfx lib logging
	logging.SetLevel(logging.ERROR, "bitfinex-ws")

	rest := rest.NewClient()

	params := websocket.NewDefaultParameters()
	ws := websocket.NewWithParams(params)

	if g.options.ApiKey != "" {
		ws.Credentials(g.options.ApiKey, g.options.ApiSecret)
		rest.Credentials(g.options.ApiKey, g.options.ApiSecret)
	}

	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("failed to load markets: %s", err)
	}

	symbolsToMarket := make(map[string]gateway.Market)
	for _, market := range g.Markets() {
		symbolsToMarket[market.Symbol] = market
	}

	authEvent := make(chan websocket.AuthEvent, 1)
	go func() {
		for obj := range ws.Listen() {
			switch data := obj.(type) {
			case error:
				log.Printf("Bfx websocket listen err: %s", data)
				panic(data)
			case *bfxbook.Book:
				g.marketDataGateway.processBookUpdate(data, symbolsToMarket)
			case *bfxbook.Snapshot:
				g.marketDataGateway.processBookSnapshot(data, symbolsToMarket)
			case *bfxtrade.Trade:
				g.marketDataGateway.processTrade(data, symbolsToMarket)
			case *websocket.AuthEvent:
				log.Printf("Bfx auth status [%s]", data.Status)
				authEvent <- *data
			case *bfxnf.Notification:
				switch data.Type {
				// Order new request
				case "on-req":
					//log.Printf("Received order on-req %+v", data)
					o := data.NotifyInfo.(bfxorder.New)
					g.accountGateway.processOrderRequest(o.ID, o.CID, data.Status, data.Text)
				// Order cancel request
				case "oc-req":
					//log.Printf("Received order oc-req %+v", data)
					o := data.NotifyInfo.(bfxorder.Cancel)
					g.accountGateway.processOrderCancel(o.ID, data.Status, data.Text)
				}
			case *bfxorder.Update:
				//log.Printf("Received bfxorder.Update %+v", data)
				g.accountGateway.processOrderUpdate(data.ID, data.CID, data.Status)
			case *bfxorder.Cancel:
				//log.Printf("Received bfxorder.Cancel %+v", data)
				g.accountGateway.processOrderUpdate(data.ID, data.CID, "CANCELED")
			case *bfxexec.TradeExecution:
				//log.Printf("Received bfxexec.TradeExecution %+v", data)
				g.accountGateway.processTradeExecution(data)
			}
		}
	}()

	err = ws.Connect()
	if err != nil {
		return err
	}

	g.marketDataGateway = NewMarketDataGateway(g.options, ws, rest, g.tickCh)
	err = g.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	g.accountGateway = NewAccountGateway(g.options, ws, rest, g.tickCh)
	if g.options.ApiKey != "" {
		err := g.accountGateway.Connect(authEvent)
		if err != nil {
			return err
		}
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
		_ = g.loadMarkets()
	}

	return g.markets
}

func (g *Gateway) loadMarkets() error {
	clientv1 := bfxv1.NewClient()
	pairs, err := clientv1.Pairs.AllDetailed()
	if err != nil {
		panic(fmt.Errorf("load detailed pairs err: %s", err))
	}

	tickers, err := g.api.Tickers()
	if err != nil {
		panic(fmt.Errorf("load tickers err: %s", err))
	}
	tickersMap := make(map[string]Ticker)
	for _, ticker := range tickers {
		tickersMap[ticker.Symbol] = ticker
	}

	g.marketsInitialized = true
	g.markets = make([]gateway.Market, len(tickers))
	for i, pair := range pairs {
		tradingSymbol := pairSymbolToTradingSymbol(pair.Pair)
		ticker, ok := tickersMap[tradingSymbol]
		if ok {
			amountPrecision := floatNumberOfDecimalPlaces(ticker.Volume)

			g.markets[i] = gateway.Market{
				Exchange:         Exchange,
				Symbol:           tradingSymbol,
				Pair:             tradingSymbolToPair(tradingSymbol),
				MakerFee:         0.001,
				TakerFee:         0.002,
				PriceTick:        1 / math.Pow10(pair.PricePrecision),
				AmountTick:       1 / math.Pow10(amountPrecision),
				MinimumOrderSize: pair.MinimumOrderSize,
			}
		}
	}

	return nil
}

// Tries to convert a float to a string and count the number of decimal places
// This is not a precise func.
func floatNumberOfDecimalPlaces(num float64) int {
	val := strconv.FormatFloat(num, 'f', -1, 64)
	parts := strings.Split(val, ".")

	if len(parts) < 2 {
		return 0
	}

	return len(parts[1])
}

// Bitfinex api returns us its pair symbols as: btcusd, ethbtc, ethusd
// But when receiving market data, it will return tBTCUSD
// The first letter means the update is for the trading order book
// and the next 6 letters are the actual symbol.
func pairSymbolToTradingSymbol(symbol string) string {
	return "t" + strings.ToUpper(symbol)
}

// Bitfinex trading instruments symbols look like: tBTCUSD, tETHBTC, tETHUSD...
// It will always have 1 character to identity the market and 3 letters
// to determine the base currency and 3 letter for the quote currency.
func tradingSymbolToPair(symbol string) gateway.Pair {
	var base string
	var quote string

	parts := strings.Split(symbol, ":")
	if len(parts) >= 2 {
		base = parts[0][1:]
		quote = parts[1]
	} else {
		base = symbol[1:4]
		quote = symbol[4:7]
	}

	return gateway.Pair{
		Base:  assetTranslateToCommon(base),
		Quote: assetTranslateToCommon(quote),
	}
}

func assetTranslateToCommon(asset string) string {
	switch asset {
	case "UST":
		return "USDT"
	}
	return asset
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}
