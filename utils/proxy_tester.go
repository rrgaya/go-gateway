package utils

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func ProxyToURL(proxy string) (*url.URL, error) {
	proxyURL, err := url.Parse(proxy)
	if err != nil {
		return nil, fmt.Errorf("invalid socks5 proxy uri [%s], url parse error: %s", proxy, err)
	}

	return proxyURL, err
}

func ProxiesToURL(list []string) (proxies []*url.URL, err error) {
	proxies = make([]*url.URL, 0)

	for _, proxy := range list {
		url, err := ProxyToURL(proxy)
		if err != nil {
			return proxies, err
		}

		proxies = append(proxies, url)
	}

	return proxies, nil
}

type proxyTestRes struct {
	proxy *url.URL
	err   error
}

func TestProxies(proxies []*url.URL) ([]*url.URL, map[*url.URL]error) {
	resCh := make(chan proxyTestRes)

	for _, p := range proxies {
		go func(proxy *url.URL) {
			client := http.Client{
				Timeout: 3 * time.Second,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
					Proxy: func(req *http.Request) (*url.URL, error) {
						return proxy, nil
					},
				},
			}

			res, err := client.Get("https://1.1.1.1/")
			if err != nil {
				resCh <- proxyTestRes{proxy, err}
				return
			}

			if res.StatusCode != 200 {
				err = fmt.Errorf("expected res status code to be 200, returned: %d", res.StatusCode)
				resCh <- proxyTestRes{proxy, err}
				return
			}

			resCh <- proxyTestRes{proxy, nil}
		}(p)
	}

	workingProxies := make([]*url.URL, 0)
	errMap := make(map[*url.URL]error)

	if len(proxies) > 0 {
		done := 0
		for res := range resCh {
			if res.err != nil {
				errMap[res.proxy] = res.err
			} else {
				workingProxies = append(workingProxies, res.proxy)
			}

			done += 1
			if done >= len(proxies) {
				break
			}
		}
	}

	return workingProxies, errMap
}

func splitStringArray(opt string) []string {
	parts := strings.Split(opt, ",")
	list := make([]string, 0)

	for _, val := range parts {
		val = strings.TrimSpace(val)
		if val != "" {
			list = append(list, val)
		}
	}

	return list
}

func ParseProxiesOpt(opt string) (proxies []*url.URL, err error) {
	list := splitStringArray(opt)

	return ProxiesToURL(list)
}

func ParseAndTestProxiesOpt(opt string) ([]*url.URL, error) {
	list := splitStringArray(opt)

	proxies, err := ProxiesToURL(list)
	if err != nil {
		return proxies, err
	}

	log.Printf("Testing [%d] proxies [%s]", len(proxies), proxies)

	working, errMap := TestProxies(proxies)
	if len(errMap) > 0 {
		for p, err := range errMap {
			log.Printf("Proxy [%s] is not working [%s]", p, err)
		}
	}

	minWorkingTolerance := 0.8 // At least 80% of proxies need to be working
	if float64(len(working))/float64(len(proxies)) < minWorkingTolerance {
		return working, fmt.Errorf("too many proxies are not working [%d/%d], need at least %.0f%% of proxies working!", len(working), len(proxies), minWorkingTolerance*100)
	}

	return working, nil
}
