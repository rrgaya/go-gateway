package whitebit

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

type apiHelper struct {
	PublicKey string
	SecretKey string
	BaseURL   string
}

func (api *apiHelper) SendRequest(requestURL string, data map[string]interface{}) (responseBody []byte, err error) {
	//If the nonce is similar to or lower than the previous request number, you will receive the 'too many requests' error message
	nonce := int(time.Now().Unix()) //nonce is a number that is always higher than the previous request number

	data["request"] = requestURL
	data["nonce"] = strconv.Itoa(nonce)
	data["nonceWindow"] = true //boolean, enable nonce validation in time range of current time +/- 5s, also check if nonce value is unique

	requestBody, err := json.Marshal(data)
	if err != nil {
		return
	}

	//preparing request URL
	completeURL := api.BaseURL + requestURL

	//calculating payload
	payload := base64.StdEncoding.EncodeToString(requestBody)

	//calculating signature using sha512
	h := hmac.New(sha512.New, []byte(api.SecretKey))
	h.Write([]byte(payload))
	signature := fmt.Sprintf("%x", h.Sum(nil))

	client := http.Client{}

	request, err := http.NewRequest("POST", completeURL, bytes.NewBuffer(requestBody))
	if err != nil {
		log.Fatal(err)
	}

	//setting neccessarry headers
	request.Header.Set("Content-type", "application/json")
	request.Header.Set("X-TXC-APIKEY", api.PublicKey)
	request.Header.Set("X-TXC-PAYLOAD", payload)
	request.Header.Set("X-TXC-SIGNATURE", signature)

	//sending request
	response, err := client.Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()

	//reciving data
	responseBody, err = ioutil.ReadAll(response.Body)

	return
}
