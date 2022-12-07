package whitebit

type TickerResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Result  struct {
		ETHBTC struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"ETH_BTC"`
		BTCUSD struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"BTC_USD"`
		ETHUSD struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"ETH_USD"`
		LTCBTC struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"LTC_BTC"`
		LTCETH struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"LTC_ETH"`
		LTCUSD struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"LTC_USD"`
		BCHRUB struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"BCH_RUB"`
		USDTUAH struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"USDT_UAH"`
		USDTUSD struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"USDT_USD"`
		USDTEUR struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"USDT_EUR"`
		BTCTUSD struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"BTC_TUSD"`
		ETHTUSD struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"ETH_TUSD"`
		BTCUSDC struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"BTC_USDC"`
		ETHUSDC struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"ETH_USDC"`
		DAIUSDT struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"DAI_USDT"`
		BTCUSDT struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"BTC_USDT"`
		XSNUSDT struct {
			At     int `json:"at"`
			Ticker struct {
				Bid    string `json:"bid"`
				Ask    string `json:"ask"`
				Low    string `json:"low"`
				High   string `json:"high"`
				Last   string `json:"last"`
				Vol    string `json:"vol"`
				Deal   string `json:"deal"`
				Change string `json:"change"`
			} `json:"ticker"`
		} `json:"XSN_USDT"`
	} `json:"result"`
}
