package gateway

import "net/url"

type Options struct {
	// Auth
	ApiKey        string `long:"apiKey" description:"API key for authentication"`
	ApiSecret     string `long:"apiSecret" description:"API secret for authentication"`
	ApiMemo       string `long:"apiMemo" description:"API memo used by some exchanges for authentication"`
	ApiPassphrase string `long:"apiPassphrase" description:"API passphrase for authentication"`
	Token         string `long:"token" description:"API token for authentication"`
	Cookie        string `long:"cookie" description:"Cookie to use when making http requests to the exchange"`
	UserAgent     string `long:"userAgent" description:"User agent to use when making http requests to the exchange"`
	// User/account id on the exchange
	UserID string `long:"userID" description:"Manually specify the userID of the user on the exchange"`
	// Connect to staging environment
	Staging bool `long:"staging" description:"Connect to staging environment"`
	// Refresh interval is an option that some gateways might use
	// when quotes (orderbook) is updated manually, via pooling.
	// When websocket intergration might not be available.
	RefreshIntervalMs int `long:"refreshIntervalMs" description:"How long to poll for updates on gtws that only support polling"`
	// Force market data polling
	PollMarketData bool `long:"pollMarketData" description:"This will force market data polling when available instead of using the websocket endpoint"`
	// Use by CurrencyLayer gateway to define the base symbol
	// when building FX quotes
	CurrencyLayerSource string   `long:"currencyLayerSource" description:"Currency layer specific option to define the base symbol"`
	LoadMarket          []string `long:"loadMarket" description:"Some exchanges, such as DEXs, we might need to manually load the desired market"`
	// Proxy servers to use
	Proxies []*url.URL
	// Define custom address to connect to
	WsAddr string `long:"wsAddr" description:"Custom websocket addr to connect to"`
	WsHost string `long:"wsHost" description:"Custom host header to use when connecting to ws"`
	// Verbose mode
	Verbose bool `long:"verbose" description:"Verbose logging mode"`
}
