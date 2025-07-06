package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"io"
	"io/ioutil"
	"log"
	"net/http"
)

type UserClient struct {
	address string
}

const (
	appJson     = "application/json"
	contentType = "Content-Type"
)

func NewUserClient(host, port string) UserClient {

	return UserClient{
		address: fmt.Sprintf("https://%s:%s", host, port),
	}
}

// GetById retrieves the details of a single user by their ID
func (client UserClient) GetById(id string) (*UserDetails, error) {
	requestURL := client.address + "/" + id
	httpReq, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		log.Println(err)
		return nil, errors.New("error while getting user details")
	}
	httpReq.Header.Set(contentType, appJson)

	clientUser, err := createTLSClient()
	res, err := clientUser.Do(httpReq)
	if err != nil || res.StatusCode != http.StatusOK {
		log.Println(err)
		log.Println(res.StatusCode)
		return nil, errors.New("error while getting user details")
	}
	otel.GetTextMapPropagator().Inject(context.Background(), propagation.HeaderCarrier(httpReq.Header))

	user := &UserDetails{}
	err = json.NewDecoder(res.Body).Decode(user)
	if err != nil {
		log.Println(err)
		return nil, errors.New("error decoding response")
	}

	return user, nil

}

func createTLSClient() (*http.Client, error) {
	caCert, err := ioutil.ReadFile("/app/cert.crt")
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{
		Transport: transport,
	}

	return client, nil
}

func (client UserClient) GetByIds(ids []string) ([]*UserDetails, error) {

	// Use the existing struct for the request body
	req := UserIdsRequest{UserIds: ids}

	// Marshal request into JSON
	reqBytes, err := json.Marshal(req)
	if err != nil {
		log.Printf("Error marshalling request body: %v", err)
		return nil, err
	}

	// Prepare the request
	bodyReader := bytes.NewReader(reqBytes)
	requestURL := client.address + "/users/details"
	httpReq, err := http.NewRequest(http.MethodPost, requestURL, bodyReader)
	if err != nil {
		log.Printf("Error creating HTTP request: %v", err)
		return nil, errors.New("error while creating the request")
	}
	httpReq.Header.Set(contentType, appJson)
	otel.GetTextMapPropagator().Inject(context.Background(), propagation.HeaderCarrier(httpReq.Header))
	// Send the request
	//res, err := http.DefaultClient.Do(httpReq)
	clientUser, err := createTLSClient()
	res, err := clientUser.Do(httpReq)
	if err != nil {
		log.Printf("Error sending request: %v", err)
		return nil, errors.New("error while sending the request")
	}
	defer res.Body.Close() // Ensure body is closed after reading

	// Check for non-OK status
	if res.StatusCode != http.StatusOK {
		log.Printf("Received non-OK status code: %d", res.StatusCode)
		body, _ := io.ReadAll(res.Body) // Read the error message body for debugging
		log.Printf("Error response body: %s", string(body))
		return nil, fmt.Errorf("error while getting user details: %s", res.Status)
	}

	// Decode the response body into users
	var users []*UserDetails
	err = json.NewDecoder(res.Body).Decode(&users)
	if err != nil {
		log.Printf("Error decoding response body: %v", err)
		return nil, errors.New("error decoding response for multiple users")
	}

	return users, nil

}

func (client UserClient) GetByIdsWithCookies(ids []string, cookie *http.Cookie) ([]*UserDetails, error) {
	if len(ids) == 0 {
		// Return an empty list if no IDs are provided
		return []*UserDetails{}, nil
	}
	// Use the existing struct for the request body
	req := UserIdsRequest{UserIds: ids}

	// Marshal request into JSON
	reqBytes, err := json.Marshal(req)
	if err != nil {
		log.Printf("Error marshalling request body: %v", err)
		return nil, err
	}

	// Prepare the request body
	bodyReader := bytes.NewReader(reqBytes)
	requestURL := client.address + "/users/details"
	httpReq, err := http.NewRequest(http.MethodPost, requestURL, bodyReader)
	httpReq.Header.Set(contentType, appJson)

	if err != nil {
		log.Printf("Error creating HTTP request: %v", err)
		return nil, errors.New("error while creating the request")
	}

	// Step 1: Attach the received cookie to the outgoing request
	httpReq.AddCookie(cookie)
	otel.GetTextMapPropagator().Inject(context.Background(), propagation.HeaderCarrier(httpReq.Header))

	// Step 2: Send the request
	clientTask, err := createTLSClient()
	res, err := clientTask.Do(httpReq)
	if err != nil {
		log.Printf("Error sending request: %v", err)
		return nil, errors.New("error while sending the request")
	}
	defer res.Body.Close() // Ensure body is closed after reading

	// Step 3: Check for non-OK status
	if res.StatusCode != http.StatusOK {
		log.Printf("Received non-OK status code: %d", res.StatusCode)
		body, _ := io.ReadAll(res.Body) // Read the error message body for debugging
		log.Printf("Error response body: %s", string(body))
		return nil, fmt.Errorf("error while getting user details: %s", res.Status)
	}

	// Step 4: Decode the response body into users
	var users []*UserDetails
	err = json.NewDecoder(res.Body).Decode(&users)
	if err != nil {
		log.Printf("Error decoding response body: %v", err)
		return nil, errors.New("error decoding response for multiple users")
	}

	return users, nil
}
