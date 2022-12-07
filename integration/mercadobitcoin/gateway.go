package mercadobitcoin

import (
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "MercadoBitcoin",
}

type Gateway struct {
	gateway.BaseGateway
	options              gateway.Options
	accountGateway       *AccountGateway
	marketDataGateway    *MarketDataGateway
	marketsInitialized   bool
	markets              []gateway.Market
	digitalAssetsMarkets map[string]bool
	tickCh               chan gateway.Tick
	tapi                 *TAPI
	apiv4                *APIV4
	accountID            string
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		tapi:                 NewTAPI(options),
		apiv4:                NewAPIV4(options),
		options:              options,
		tickCh:               make(chan gateway.Tick, 10),
		markets:              make([]gateway.Market, 0),
		digitalAssetsMarkets: make(map[string]bool),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	if g.options.ApiKey != "" {
		err = g.authenticateAPIV4()
		if err != nil {
			return fmt.Errorf("Failed to init api v4 authentication, err: %s", err)
		}

		accounts, err := g.apiv4.ListAccounts()
		if err != nil {
			return fmt.Errorf("Failed to select account id from v4 api, err: %s", err)
		}

		if len(accounts) == 0 {
			return fmt.Errorf("APIV4 didnt return any account, cant init APIV4 wihthout account id")
		}

		g.accountID = accounts[0].ID

		log.Printf("MercadoBitcoin returned [%d] accounts, using accounts[0] id [%s]", len(accounts), g.accountID)
	}

	g.accountGateway = NewAccountGateway(g.options, g.Markets(), g.tapi, g.tickCh, g.apiv4, g.accountID)
	if g.options.ApiKey != "" {
		err := g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("Failed to connect to account gateway, err %s", err)
		}
	}

	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh, g.digitalAssetsMarkets)

	return nil
}

func (g *Gateway) authenticateAPIV4() error {
	err, expiresIn := g.renewAPIV4AccessToken()
	if err != nil {
		return err
	}

	go func() {
		for {
			time.Sleep(expiresIn)

			err, expiresIn = g.renewAPIV4AccessToken()
			if err != nil {
				log.Printf("Failed to renew api v4 access token [err: %s], retrying in 5 seconds...", err)
				time.Sleep(5 * time.Second)
				continue
			}
		}
	}()

	return nil
}

func (g *Gateway) renewAPIV4AccessToken() (error, time.Duration) {
	auth, err := g.apiv4.Authorize(g.options.ApiKey, g.options.ApiSecret)
	if err != nil {
		return err, 0
	}

	if auth.AccessToken == "" {
		return fmt.Errorf("access token returned empty"), 0
	}

	expiresAt := time.Unix(auth.Expiration, 0)
	expiresIn := time.Until(expiresAt)

	log.Printf("MercadoBitcoin setting APIV4 access token [%s] expires in [%s]", auth.AccessToken, expiresIn)
	g.apiv4.SetAccessToken(auth.AccessToken)

	return nil, expiresIn
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

var defaultAmountTick = 0.00000001
var defaultPriceTick = 0.00001
var customPriceTicks = map[string]float64{
	"SHIB": 0.00000001,
}

func (g *Gateway) loadMarkets() error {
	symbols, err := g.apiv4.Symbols()
	if err != nil {
		return fmt.Errorf("failed to load pairs: %s", err)
	}

	markets := make([]gateway.Market, len(symbols.BaseCurrency))
	for i, base := range symbols.BaseCurrency {
		quote := symbols.Currency[i]
		amountTick := defaultAmountTick
		priceTick, priceTickOk := customPriceTicks[base]
		if !priceTickOk {
			priceTick = defaultPriceTick
		}

		sym := fmt.Sprintf("%s%s", quote, base)

		markets[i] = gateway.Market{
			Exchange:               Exchange,
			Pair:                   gateway.Pair{Base: base, Quote: quote},
			Symbol:                 sym,
			PriceTick:              priceTick,
			AmountTick:             amountTick,
			MakerFee:               0.0025,
			TakerFee:               0.0075,
			MinimumOrderMoneyValue: 1, // 1 Real
		}

		// Mark this market as a digital asset
		// because we will need to subscribe to a different market data for it
		_type := symbols.Type[i]
		if _type == "DIGITAL_ASSET" {
			g.digitalAssetsMarkets[sym] = true
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
