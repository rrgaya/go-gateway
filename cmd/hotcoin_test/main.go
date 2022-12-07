package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/integration/hotcoin"
)

func main() {
	ws := hotcoin.NewWsSession()

	err := ws.Connect()
	if err != nil {
		log.Fatalln(err)
	}

	data, err := json.Marshal(hotcoin.WsRequest{Sub: "market.btc_usdt.trade.depth"})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Request data", string(data))

	err = ws.WriteMessage(data)
	if err != nil {
		log.Fatalln(err)
	}

	time.Sleep(1000000000 * time.Millisecond)
}
