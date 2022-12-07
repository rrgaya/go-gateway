package mapper

import (
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/integration/biconomy"
	"github.com/herenow/atomic-gtw/integration/bigone"
	"github.com/herenow/atomic-gtw/integration/binance"
	"github.com/herenow/atomic-gtw/integration/binancefutures"
	"github.com/herenow/atomic-gtw/integration/bitcointrade"
	"github.com/herenow/atomic-gtw/integration/bitfinex"
	"github.com/herenow/atomic-gtw/integration/bitforex"
	"github.com/herenow/atomic-gtw/integration/bitglobal"
	"github.com/herenow/atomic-gtw/integration/bitmart"
	"github.com/herenow/atomic-gtw/integration/bitmex"
	"github.com/herenow/atomic-gtw/integration/bitrue"
	"github.com/herenow/atomic-gtw/integration/bitso"
	"github.com/herenow/atomic-gtw/integration/bittrex"
	"github.com/herenow/atomic-gtw/integration/bitz"
	"github.com/herenow/atomic-gtw/integration/bkex"
	"github.com/herenow/atomic-gtw/integration/chiliz"
	"github.com/herenow/atomic-gtw/integration/coinbene"
	"github.com/herenow/atomic-gtw/integration/coinstore"
	"github.com/herenow/atomic-gtw/integration/cointiger"
	"github.com/herenow/atomic-gtw/integration/currencylayer"
	"github.com/herenow/atomic-gtw/integration/digifinex"
	"github.com/herenow/atomic-gtw/integration/digitra"
	"github.com/herenow/atomic-gtw/integration/exmarkets"
	"github.com/herenow/atomic-gtw/integration/fmfw"
	"github.com/herenow/atomic-gtw/integration/foxbit"
	"github.com/herenow/atomic-gtw/integration/ftx"
	"github.com/herenow/atomic-gtw/integration/gateio"
	"github.com/herenow/atomic-gtw/integration/gemini"
	"github.com/herenow/atomic-gtw/integration/hotbit"
	"github.com/herenow/atomic-gtw/integration/hotcoin"
	"github.com/herenow/atomic-gtw/integration/huobi"
	"github.com/herenow/atomic-gtw/integration/kucoin"
	"github.com/herenow/atomic-gtw/integration/latoken"
	"github.com/herenow/atomic-gtw/integration/lbank"
	"github.com/herenow/atomic-gtw/integration/mercadobitcoin"
	"github.com/herenow/atomic-gtw/integration/mexc"
	"github.com/herenow/atomic-gtw/integration/novadax"
	"github.com/herenow/atomic-gtw/integration/p2pb2b"
	"github.com/herenow/atomic-gtw/integration/pancakeswap"
	"github.com/herenow/atomic-gtw/integration/probit"
	"github.com/herenow/atomic-gtw/integration/ripio"
	"github.com/herenow/atomic-gtw/integration/upbit"
	"github.com/herenow/atomic-gtw/integration/whitebit"
	"github.com/herenow/atomic-gtw/integration/xt"
)

func init() {
	gateway.RegisterExchange(hotbit.Exchange, hotbit.NewGateway)
	gateway.RegisterExchange(latoken.Exchange, latoken.NewGateway)
	gateway.RegisterExchange(huobi.Exchange, huobi.NewGateway)
	gateway.RegisterExchange(bitfinex.Exchange, bitfinex.NewGateway)
	gateway.RegisterExchange(binance.Exchange, binance.NewGateway)
	gateway.RegisterExchange(binancefutures.Exchange, binancefutures.NewGateway)
	gateway.RegisterExchange(bitforex.Exchange, bitforex.NewGateway)
	gateway.RegisterExchange(ripio.Exchange, ripio.NewGateway)
	gateway.RegisterExchange(currencylayer.Exchange, currencylayer.NewGateway)
	gateway.RegisterExchange(bitz.Exchange, bitz.NewGateway)
	gateway.RegisterExchange(bitmex.Exchange, bitmex.NewGateway)
	gateway.RegisterExchange(upbit.Exchange, upbit.NewGateway)
	gateway.RegisterExchange(mercadobitcoin.Exchange, mercadobitcoin.NewGateway)
	gateway.RegisterExchange(bitcointrade.Exchange, bitcointrade.NewGateway)
	gateway.RegisterExchange(coinbene.Exchange, coinbene.NewGateway)
	gateway.RegisterExchange(probit.Exchange, probit.NewGateway)
	gateway.RegisterExchange(exmarkets.Exchange, exmarkets.NewGateway)
	gateway.RegisterExchange(p2pb2b.Exchange, p2pb2b.NewGateway)
	gateway.RegisterExchange(chiliz.Exchange, chiliz.NewGateway)
	gateway.RegisterExchange(gateio.Exchange, gateio.NewGateway)
	gateway.RegisterExchange(bitso.Exchange, bitso.NewGateway)
	gateway.RegisterExchange(foxbit.Exchange, foxbit.NewGateway)
	gateway.RegisterExchange(ftx.Exchange, ftx.NewGateway)
	gateway.RegisterExchange(novadax.Exchange, novadax.NewGateway)
	gateway.RegisterExchange(gemini.Exchange, gemini.NewGateway)
	gateway.RegisterExchange(mexc.Exchange, mexc.NewGateway)
	gateway.RegisterExchange(pancakeswap.Exchange, pancakeswap.NewGateway)
	gateway.RegisterExchange(digifinex.Exchange, digifinex.NewGateway)
	gateway.RegisterExchange(bigone.Exchange, bigone.NewGateway)
	gateway.RegisterExchange(lbank.Exchange, lbank.NewGateway)
	gateway.RegisterExchange(bitmart.Exchange, bitmart.NewGateway)
	gateway.RegisterExchange(kucoin.Exchange, kucoin.NewGateway)
	gateway.RegisterExchange(xt.Exchange, xt.NewGateway)
	gateway.RegisterExchange(bitglobal.Exchange, bitglobal.NewGateway)
	gateway.RegisterExchange(bitrue.Exchange, bitrue.NewGateway)
	gateway.RegisterExchange(coinstore.Exchange, coinstore.NewGateway)
	gateway.RegisterExchange(cointiger.Exchange, cointiger.NewGateway)
	gateway.RegisterExchange(digitra.Exchange, digitra.NewGateway)
	gateway.RegisterExchange(fmfw.Exchange, fmfw.NewGateway)
	gateway.RegisterExchange(bittrex.Exchange, bittrex.NewGateway)
	gateway.RegisterExchange(hotcoin.Exchange, hotcoin.NewGateway)
	gateway.RegisterExchange(bkex.Exchange, bkex.NewGateway)
	gateway.RegisterExchange(biconomy.Exchange, biconomy.NewGateway)
	gateway.RegisterExchange(whitebit.Exchange, whitebit.NewGateway)
}
