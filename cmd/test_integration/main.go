package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	_ "github.com/herenow/atomic-gtw/mapper"
	"github.com/jessevdk/go-flags"
	_ "github.com/lib/pq"
)

type Options struct {
	gateway.Options
	SubscribeMarkets bool          `long:"subscribeMarkets" description:"Subscribe to market data updates"`
	CancelOpenOrders bool          `long:"cancelOpenOrders" description:"Cancel open orders"`
	Exchange         string        `long:"exchange" description:"Exchange to look for arbitrages"`
	Symbol           string        `long:"symbol" description:"Test order market symbol symbol"`
	Price            float64       `long:"price" description:"Test order price"`
	Amount           float64       `long:"amount" description:"Test order amount"`
	WaitBeforeCancel time.Duration `long:"waitBeforeCancel" default:"5s"`
	WaitBeforeExit   time.Duration `long:"waitBeforeExit" default:"5s"`
	Buy              bool          `long:"buy" description:"Buy test order"`
	Sell             bool          `long:"sell" description:"Sell test order"`
	PgDB             string        `long:"pgDB" description:"Postgres database uri"`
	BotID            string        `long:"botID" description:"Bot identification on DB"`
}

var opts Options

var optsParser = flags.NewParser(&opts, flags.Default)

func main() {
	// Parse flags
	if _, err := optsParser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type != flags.ErrHelp {
			fmt.Printf("Failed to parse args, err:\n%s\n", err)
		}
		os.Exit(0)
	}

	if opts.Exchange == "" || opts.Symbol == "" || opts.Price == 0 || opts.Amount == 0 {
		log.Fatal("You must pass an --exchange --symbol --price --amount")
	}

	if opts.Buy == false && opts.Sell == false {
		log.Fatal("You must pass a --buy or --sell flag")
	}

	if opts.BotID == "" {
		opts.BotID = "test_integration"
	}

	exchange, ok := gateway.ExchangeBySymbol(opts.Exchange)
	if !ok {
		log.Printf("Failed to find exchange \"%s\"", opts.Exchange)
		log.Printf("Available exchange are:")
		for _, exchange := range gateway.Exchanges {
			log.Printf("- %s", exchange.Symbol())
		}
		return
	}

	options := gateway.Options{
		ApiKey:        opts.ApiKey,
		ApiSecret:     opts.ApiSecret,
		ApiMemo:       opts.ApiMemo,
		ApiPassphrase: opts.ApiPassphrase,
		Token:         opts.Token,
		Cookie:        opts.Cookie,
		UserID:        opts.UserID,
		UserAgent:     opts.UserAgent,
		Staging:       opts.Staging,
		Verbose:       opts.Verbose,
	}
	gtw, ok := gateway.NewByExchange(exchange, options)
	if !ok {
		log.Printf("Failed to find gateway for exchange %s", exchange)
		return
	}

	// Connect gtw
	err := gtw.Connect()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Connected to exchange...")

	log.Printf("Available markets:\n%+v", gtw.Markets())

	// Find markets
	markets := gtw.Markets()
	market, ok := gateway.MarketBySymbol(markets, opts.Symbol)
	if !ok {
		log.Printf("Available markets at %s: %s", gtw.Exchange(), gateway.MarketsToSymbols(markets))
		log.Fatalf("Failed to locate symbol %s", opts.Symbol)
	}

	// Print balances
	balances, err := gtw.AccountGateway().Balances()
	if err != nil {
		log.Printf("Failed to gtw.AccountGateway().Balances(), err: %s", err)
	} else {
		log.Printf("Returned balances: %d", len(balances))
		log.Printf("Printing balances w/ total > 0")
		for _, b := range balances {
			if b.Total > 0 {
				log.Printf("%s : Total %f : Avail %f", b.Asset, b.Total, b.Available)
			}
		}
	}

	// Subscribe to market updates
	if opts.SubscribeMarkets {
		err = gtw.SubscribeMarkets([]gateway.Market{market})
		if err != nil {
			log.Printf("Failed to SubscribeMarkets(), err: %s", err)
		}
	}

	// Tick processor
	go func() {
		for tick := range gtw.Tick() {
			log.Printf("Received tick update:\n%s", tick)
		}
	}()

	orders, err := gtw.AccountGateway().OpenOrders(market)
	if err != nil {
		log.Fatalf("err: %s", err)
	}

	log.Printf("Open orders")
	for _, order := range orders {
		log.Printf("Order: %s", order)

		if opts.CancelOpenOrders {
			log.Printf("Cancelling order [%s]...", order)
			err := gtw.AccountGateway().CancelOrder(order)
			if err != nil {
				log.Printf("Failed to cancel order [%s], err: %s", order, err)
			}
		}
	}

	var side gateway.Side
	if opts.Buy {
		side = gateway.Bid
	} else if opts.Sell {
		side = gateway.Ask
	}

	// Try to place order
	order := gateway.Order{
		Market: market,
		Side:   side,
		Price:  opts.Price,
		Amount: opts.Amount,
	}

	log.Printf("Opening order %s in 3 seconds...", order)

	time.Sleep(3 * time.Second)

	reqAt := time.Now()
	orderId, err := gtw.AccountGateway().SendOrder(order)
	if err != nil {
		log.Fatalf("Failed to send order %+v, err: %s", order, err)
	}

	order.ID = orderId

	log.Printf("Successfully sent order %+v, order id: %s, response time: %v", order, orderId, time.Now().Sub(reqAt))
	log.Printf("Waiting %v seconds before cancelling order...", opts.WaitBeforeCancel)
	time.Sleep(opts.WaitBeforeCancel)

	reqAt = time.Now()
	err = gtw.AccountGateway().CancelOrder(order)
	if err != nil {
		log.Fatalf("Failed to cancel order, err: %s", err)
	}

	log.Printf("Canceled order, response time %v!", time.Now().Sub(reqAt))
	log.Printf("Waiting %v seconds before exiting...", opts.WaitBeforeExit)
	time.Sleep(opts.WaitBeforeExit)
}
