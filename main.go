package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/gwynethguo/deribit-kafka-orderbook/constants"
	"github.com/gwynethguo/deribit-kafka-orderbook/handler"
	"github.com/gwynethguo/deribit-kafka-orderbook/logging"
	"github.com/segmentio/kafka-go"
)

type InstrumentDetails struct {
	ch     chan handler.DeribitMessage
	cancel context.CancelFunc
}

// handleOrderBookSubscriptions subscribes or unsubscribes from order book channels on the Deribit WebSocket.
func handleOrderBookSubscriptions(conn *websocket.Conn, instruments []string, isSubscribe bool) handler.SubscribeResponse {
	// Determine the subscription method (subscribe/unsubscribe)
	method := "public/unsubscribe"
	if isSubscribe {
		method = "public/subscribe"
	}

	// Construct subscription request
	subscribeMsg := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params": map[string]any{
			"channels": instruments,
		},
	}

	// Send the subscription request
	if err := conn.WriteJSON(subscribeMsg); err != nil {
		log.Printf("Failed to send subscribe message: %v\n", err)
	}

	// Read the response from WebSocket
	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Printf("Failed to read WebSocket message: %v.", err)
	}

	log.Println(string(msg))

	// Parse the response into SubscribeResponse struct
	var responseMessage handler.SubscribeResponse
	if err = json.Unmarshal(msg, &responseMessage); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
	}

	// Log any errors from the response
	if responseMessage.Error.Code != 0 {
		log.Printf("Subscribe response error: %d %s", responseMessage.Error.Code, responseMessage.Error.Message)
	}

	return responseMessage
}

// getInstruments fetches available BTC options instruments from Deribit API.
func getInstruments() []string {
	resp, err := http.Get(fmt.Sprintf("%s?currency=BTC&kind=option&expired=false", constants.DeribitAPIURL))
	if err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var response handler.InstrumentResponse
	if err := json.Unmarshal(body, &response); err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v\n", err)
	}

	// Check for API response errors
	if response.Error.Code != 0 {
		log.Fatalf("Error detected in response: Code=%d, Message=%s\n", response.Error.Code, response.Error.Message)
	}

	// Extract instrument names from response
	var instruments []string
	for _, instrument := range response.Result {
		instruments = append(instruments, instrument.InstrumentName)
	}

	return instruments
}
func handleInstrumentUpdates(ctx context.Context, wg *sync.WaitGroup, prevInstruments []string, updateCh chan<- handler.InstrumentUpdate) {
	prevInstrumentsMap := make(map[string]struct{})
	for _, instrument := range prevInstruments {
		prevInstrumentsMap[instrument] = struct{}{}
	}

	for {
		select {
		case <-ctx.Done():
			close(updateCh)
			return
		default:
			response := getInstruments()

			instruments := make(map[string]struct{})
			for _, instrument := range response {
				_, ok := prevInstrumentsMap[instrument]
				if !ok {
					wg.Add(1)
					go func() {
						defer wg.Done()
						updateCh <- handler.InstrumentUpdate{Instrument: instrument, UpdateType: handler.ADD}
					}()
				}
				instruments[instrument] = struct{}{}
			}

			for instrument, _ := range prevInstrumentsMap {
				_, ok := instruments[instrument]
				if !ok {
					wg.Add(1)
					go func() {
						defer wg.Done()
						updateCh <- handler.InstrumentUpdate{Instrument: instrument, UpdateType: handler.DELETE}
					}()
				}
			}

			prevInstrumentsMap = instruments
		}
	}
}

func main() {
	// Initialize logging to a file
	logFile := logging.WriteLogsToFile()
	defer logFile.Close()

	// Initialize Kafka writer
	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        constants.KafkaTopic,
		RequiredAcks: kafka.RequireAll,
	}
	defer kafkaWriter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Map to store instrument-specific channels
	instrumentHandlerMap := make(map[string]InstrumentDetails)

	// Channel for handling instrument updates
	updateCh := make(chan handler.InstrumentUpdate)

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			break
		default:
			conn, _, err := websocket.DefaultDialer.Dial(constants.DeribitWSURL, nil)
			if err != nil {
				log.Printf("Failed to connect to Deribit: %v\n", err)
			}

			// Retrieve available instruments
			instruments := getInstruments()

			for i, instrument := range instruments {
				instruments[i] = fmt.Sprintf("book.%s.100ms", instrument)
			}

			var subscribedInstruments []string
			// Subscribe to instruments in batches of 50
			for i := 0; i < len(instruments); i += 50 {
				batch := instruments[i:min(i+50, len(instruments))]
				response := handleOrderBookSubscriptions(conn, batch, true)
				subscribedInstruments = append(subscribedInstruments, response.Result...)
			}

			go handleInstrumentUpdates(ctx, &wg, subscribedInstruments, updateCh)

			for {
				select {
				case <-ctx.Done():
					for _, instrumentDetails := range instrumentHandlerMap {
						instrumentDetails.cancel()
						close(instrumentDetails.ch)
					}
					break
				case instrumentUpdate := <-updateCh:
					switch instrumentUpdate.UpdateType {
					case handler.DELETE:
						instrumentDetails, ok := instrumentHandlerMap[instrumentUpdate.Instrument]
						handleOrderBookSubscriptions(conn, []string{fmt.Sprintf("book.%s.100ms", instrumentUpdate.Instrument)}, false)

						if !ok {
							continue
						}
						// cancel child context
						close(instrumentDetails.ch)
						instrumentDetails.cancel()
						// remove from map
						delete(instrumentHandlerMap, instrumentUpdate.Instrument)
					case handler.ADD:
						_, ok := instrumentHandlerMap[instrumentUpdate.Instrument]
						if ok {
							continue
						}
						handleOrderBookSubscriptions(conn, []string{fmt.Sprintf("book.%s.100ms", instrumentUpdate.Instrument)}, true)
					case handler.RESUBSCRIBE:
						_, ok := instrumentHandlerMap[instrumentUpdate.Instrument]
						if !ok {
							continue
						}
						handleOrderBookSubscriptions(conn, []string{fmt.Sprintf("book.%s.100ms", instrumentUpdate.Instrument)}, false)
						handleOrderBookSubscriptions(conn, []string{fmt.Sprintf("book.%s.100ms", instrumentUpdate.Instrument)}, true)
					}
				default:
					_, msg, err := conn.ReadMessage()
					if err != nil {
						log.Printf("Failed to read Websocket message: %v.", err)
						break
					}

					var deribitMsg handler.DeribitMessage
					if err := json.Unmarshal(msg, &deribitMsg); err != nil {
						log.Printf("Failed to unmarshal message: %v", err)
						continue
					}

					// Skip
					if deribitMsg.Error.Code != 0 {
						continue
					}

					// Skip non-orderbook messages
					if deribitMsg.Method != "subscription" {
						continue
					}

					// Check if a handler exists for this instrument, if not, create one
					_, exists := instrumentHandlerMap[deribitMsg.Params.Data.Instrument]
					if !exists {
						var instrumentHandler handler.InstrumentHandler
						instrumentCh := make(chan handler.DeribitMessage)
						childCtx, childCancel := context.WithCancel(ctx)
						instrumentHandler.Init(childCtx, &wg, kafkaWriter, instrumentCh, updateCh)
						instrumentHandlerMap[deribitMsg.Params.Data.Instrument] = InstrumentDetails{ch: instrumentCh, cancel: childCancel}
					}
					instrumentCh := instrumentHandlerMap[deribitMsg.Params.Data.Instrument].ch
					wg.Add(1)
					go func() {
						defer wg.Done()
						instrumentCh <- deribitMsg
					}()
				}
			}
		}
	}
}
