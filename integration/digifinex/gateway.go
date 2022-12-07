package digifinex

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

var Exchange = gateway.Exchange{
	Name: "DigiFinex",
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
	err := g.loadMarketsFromWebAPI()
	if err != nil {
		return fmt.Errorf("load markets: %s", err)
	}

	g.accountGateway = NewAccountGateway(g.api, g.options, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("account gtw connect: %s", err)
		}
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

		err := g.accountGateway.subscribeMarketsUpdate(markets)
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
		_ = g.loadMarketsFromWebAPI()
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

// TODO: The regular api is not returning all pairs, we will use the
// websites api for now
type WebAPIPairRes struct {
	Errcode int `json:"errcode"`
	Data    struct {
		List []WebAPIPair `json:"list"`
	} `json:"data"`
}

type WebAPIPair struct {
	TradePair       string  `json:"trade_pair"`
	PricePrecision  int     `json:"price_precision"`
	AmountPrecision int     `json:"amount_precision"`
	AmountMinimum   float64 `json:"amount_minimum,string"` // Min order amount
	MinVolume       float64 `json:"min_volume,string"`     // Min order money value
}

func (g *Gateway) loadMarketsFromWebAPI() error {
	client := utils.NewHttpClient()
	client.UseProxies(g.options.Proxies)

	req, err := http.NewRequest(http.MethodGet, "https://api.digifinex.com/order/limit", nil)
	if err != nil {
		return fmt.Errorf("http new req err: %s", err)
	}

	res, err := client.SendRequest(req)
	if err != nil {
		return fmt.Errorf("http send req err: %s", err)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("http read body err: %s", err)
	}

	var apiRes WebAPIPairRes
	err = json.Unmarshal(body, &apiRes)
	if err != nil {
		return fmt.Errorf("http body unmarshal err: %s", err)
	}

	if apiRes.Errcode != 0 {
		return fmt.Errorf("load markets api err code res: %d, body: %s", apiRes.Errcode, string(body))
	}

	g.marketsInitialized = true
	commonMarkets := make([]gateway.Market, 0)
	for _, market := range apiRes.Data.List {

		symbol := market.TradePair
		parts := strings.Split(symbol, "_")
		if len(parts) < 2 {
			err = fmt.Errorf("DigiFinex failed to translate symbol [%s] into base/quote", symbol)
			log.Println(err)
			continue
		}
		base := strings.ToUpper(parts[0])
		quote := strings.ToUpper(parts[1])
		priceTick := 1 / math.Pow10(int(market.PricePrecision))
		amountTick := 1 / math.Pow10(int(market.AmountPrecision))

		commonMarkets = append(commonMarkets, gateway.Market{
			Exchange: Exchange,
			Pair: gateway.Pair{
				Base:  base,
				Quote: quote,
			},
			Symbol:                 strings.ToLower(symbol),
			PriceTick:              priceTick,
			AmountTick:             amountTick,
			MinimumOrderSize:       market.AmountMinimum,
			MinimumOrderMoneyValue: market.MinVolume,
		})
	}
	g.markets = commonMarkets

	return nil
}

func marketsToCommonMarket(markets []APIMarket) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(markets))

	for _, market := range markets {
		mkt, err := marketToCommonMarket(market)
		if err == nil {
			commonMarkets = append(commonMarkets, mkt)
		}
	}

	return commonMarkets
}

func marketToCommonMarket(market APIMarket) (mkt gateway.Market, err error) {
	symbol := market.Market
	parts := strings.Split(symbol, "_")
	if len(parts) < 2 {
		err = fmt.Errorf("DigiFinex failed to translate symbol [%s] into base/quote", symbol)
		log.Println(err)
		return mkt, err
	}
	base := strings.ToUpper(parts[0])
	quote := strings.ToUpper(parts[1])
	priceTick := 1 / math.Pow10(int(market.PricePrecision))
	amountTick := 1 / math.Pow10(int(market.VolumePrecision))

	return gateway.Market{
		Exchange: Exchange,
		Pair: gateway.Pair{
			Base:  base,
			Quote: quote,
		},
		Symbol:                 symbol,
		TakerFee:               0.0025,
		MakerFee:               0.00,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       market.MinAmount,
		MinimumOrderMoneyValue: market.MinVolume,
	}, nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
