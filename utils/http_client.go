package utils

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"
)

type HttpClient struct {
	proxies     []*url.URL
	proxyCursor int
	lock        *sync.RWMutex
	httpClient  http.Client
	transport   *http.Transport
}

func NewHttpClient() *HttpClient {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	return &HttpClient{
		proxies:   make([]*url.URL, 0),
		lock:      &sync.RWMutex{},
		transport: transport,
		httpClient: http.Client{
			Transport: transport,
			// Dont follow redirects
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (c *HttpClient) getNextProxy(req *http.Request) (*url.URL, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	c.proxyCursor += 1
	proxy := c.proxies[c.proxyCursor%len(c.proxies)]

	return proxy, nil
}

func (c *HttpClient) UseProxies(proxies []*url.URL) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, proxy := range proxies {
		c.proxies = append(c.proxies, proxy)
	}

	if len(c.proxies) > 0 {
		c.transport.Proxy = c.getNextProxy
	}

	return nil
}

func (c *HttpClient) SendRequest(req *http.Request) (*http.Response, error) {
	return c.httpClient.Do(req)
}
