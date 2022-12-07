package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"

	tm "github.com/buger/goterm"
	"github.com/fatih/color"
	"github.com/herenow/atomic-gtw/book"
	"github.com/herenow/atomic-gtw/gateway"
	_ "github.com/herenow/atomic-gtw/mapper"
	"github.com/herenow/atomic-gtw/utils"
	"github.com/jessevdk/go-flags"
)

type Options struct {
	gateway.Options
	Exchange            string `long:"exchange" description:"Exchange symbol to connect to"`
	Symbol              string `long:"symbol" description:"Market symbol to display orderbook for"`
	Depth               int    `long:"depth" description:"Book depth to print"`
	SubscribeAllMarkets bool   `long:"subscribeAllMarkets"`
}

var opts Options

var optsParser = flags.NewParser(&opts, flags.Default)

func main() {
	// Parse flags
	if _, err := optsParser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			log.Fatalf("failed to parse flags, err: %s", err)
		} else {
			log.Fatalf("invalid flags, err: %s", err)
		}
	}

	if opts.Exchange == "" {
		log.Fatal("You must pass --exchange to connect to")
	}

	if opts.Symbol == "" {
		log.Fatal("You must pass market --symbol pair to display orderbook for")
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
		ApiKey:     opts.ApiKey,
		ApiSecret:  opts.ApiSecret,
		Token:      opts.Token,
		Cookie:     opts.Cookie,
		UserID:     opts.UserID,
		UserAgent:  opts.UserAgent,
		Staging:    opts.Staging,
		Verbose:    opts.Verbose,
		LoadMarket: opts.LoadMarket,
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

	// Find markets
	markets := gtw.Markets()
	market, ok := gateway.MarketBySymbol(markets, opts.Symbol)
	if !ok {
		log.Printf("Available markets at %s: %s", gtw.Exchange(), gateway.MarketsToSymbols(markets))
		log.Fatalf("Failed to locate symbol %s", opts.Symbol)
	}

	// Subscribe market data
	if opts.SubscribeAllMarkets {
		err = gtw.SubscribeMarkets(gtw.Markets())
		if err != nil {
			log.Printf("Failed to SubscribeMarkets(allMarkets), err: %s", err)
		}
	} else {
		err = gtw.SubscribeMarkets([]gateway.Market{market})
		if err != nil {
			log.Printf("Failed to SubscribeMarkets(market), err: %s", err)
		}
	}
	if err != nil && err != gateway.NotImplementedErr {
		log.Fatalf("Failed to subscribe to market data: %s", err)
	}

	// Order book maintainer
	ob := book.NewBook(market.Symbol)
	go func() {
		for tick := range gtw.Tick() {
			_ = updateBookFromTick(market, ob, tick)
		}
	}()

	// Display orderbook
	log.Printf("Succesfully connected to gateway, initializating order book viewer in 5 seconds...")
	time.Sleep(5 * time.Second)

	go func() {
		tm.Clear()
		log.SetOutput(ioutil.Discard)

		depth := opts.Depth
		if depth <= 0 {
			depth = 10
		}

		for {
			repaint(market, depth, ob)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Commands
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			text := scanner.Text()
			switch text {
			default:
				tm.Clear()
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatalf("scan failed err: %s", err)
		}
	}()

	// Wait for interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
}

func repaint(market gateway.Market, depth int, ob *book.Book) {
	tm.MoveCursor(1, 1)

	fmt.Printf("%s - %s\n", market, time.Now())
	fmt.Printf(" %30s | %-30s\n", "Bids", "Asks")
	fmt.Printf("---------------------------------------------------------------------------------\n")
	fmt.Printf("%-15s %15s | %-15s %15s\n", "Amount", "Price", "Price", "Amount")

	stdPrinter := color.New(color.FgWhite, color.BgBlack)

	for n := 0; n < depth; n++ {
		bid, _ := ob.Bids.TopN(n)
		printer := stdPrinter
		printer.Printf(fmt.Sprintf(
			"%-15s %15s",
			utils.FloatToStringWithTick(bid.Amount, market.AmountTick),
			utils.FloatToStringWithTick(bid.Value, market.PriceTick),
		))

		fmt.Printf(" | ")

		ask, _ := ob.Asks.TopN(n)
		printer = stdPrinter
		printer.Printf(fmt.Sprintf(
			"%-15s %15s",
			utils.FloatToStringWithTick(ask.Value, market.PriceTick),
			utils.FloatToStringWithTick(ask.Amount, market.AmountTick),
		))

		fmt.Printf("\n")
	}

	fmt.Printf("---------------------------------------------------------------------------------\n")

	bestBid, _ := ob.Bids.Top()
	bestAsk, _ := ob.Asks.Top()

	fmt.Printf(
		"Bid/Ask spread: %s\n",
		utils.FloatToStringWithTick((bestAsk.Value-bestBid.Value), market.PriceTick),
	)

	fmt.Printf("---------------------------------------------------------------------------------\n")

	tm.Flush() // Call it every time at the end of rendering
}

func updateBookFromTick(market gateway.Market, ob *book.Book, tick gateway.Tick) bool {
	var snapshotSequence bool
	var bids []book.Price
	var asks []book.Price

	events := gateway.FilterEventsBySymbol(market.Symbol, tick.EventLog)

	for _, event := range events {
		if event.Type == gateway.SnapshotSequenceEvent {
			snapshotSequence = true
		} else if event.Type == gateway.DepthEvent {
			depth := event.Data.(gateway.Depth)
			if depth.Side == gateway.Bid {
				bids = append(bids, book.Price{Value: depth.Price, Amount: depth.Amount})
			} else {
				asks = append(asks, book.Price{Value: depth.Price, Amount: depth.Amount})
			}
		}
	}

	if len(bids) > 0 || len(asks) > 0 || snapshotSequence {
		if snapshotSequence {
			ob.Snapshot(bids, asks)
		} else {
			ob.DeltaUpdate(bids, asks)
		}

		return true
	}

	return false
}
