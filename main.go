package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/gwynethguo/deribit-kafka-orderbook/constants"
	"github.com/gwynethguo/deribit-kafka-orderbook/handler"
	"github.com/gwynethguo/deribit-kafka-orderbook/logging"
	"github.com/segmentio/kafka-go"
)

func main() {
	logFile := logging.WriteLogsToFile()
	defer logFile.Close()

	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        constants.KafkaTopic,
		RequiredAcks: kafka.RequireAll,
	}
	defer kafkaWriter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	instrumentHandlerMap := make(map[string]chan handler.DeribitMessage)

	reconnectCh := make(chan string)

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
		default:
			conn, _, err := websocket.DefaultDialer.Dial(constants.DeribitWSURL, nil)
			if err != nil {
				log.Printf("Failed to connect to Deribit: %v\n", err)
			}

			instruments := []string{"BTC-30MAY25-74000-C", "BTC-30MAY25-74000-P", "BTC-4APR25-97000-C", "BTC-27JUN25-44000-P"}
			channels := make([]string, len(instruments))

			for i, instrument := range instruments {
				channels[i] = fmt.Sprintf("book.%s.100ms", instrument)
			}

			subscribeMsg := map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"method":  "public/subscribe",
				"params": map[string]any{
					"channels": channels,
				},
			}
			if err := conn.WriteJSON(subscribeMsg); err != nil {
				log.Printf("Subscribe failed: %v", err)
			}

			_, msg, err := conn.ReadMessage()
			// TODO: handle instrument doesn't exist
			if err != nil {
				log.Printf("Websocket read error: %v.", err)
				continue
			}

			log.Println(string(msg))

			for {
				select {
				case <-ctx.Done():
					break
				// case instrumentName := <-reconnectCh:
				//  reconnect for the instrument given
				default:
					_, msg, err = conn.ReadMessage()
					if err != nil {
						log.Printf("Websocket read error: %v.", err)
						break
					}

					log.Printf("msg = %s\n", string(msg))

					var deribitMsg handler.DeribitMessage
					if err := json.Unmarshal(msg, &deribitMsg); err != nil {
						log.Printf("Failed to unmarshal message: %v", err)
						continue
					}

					log.Println(deribitMsg)

					if deribitMsg.Method != "subscription" {
						continue // Skip non-orderbook messages
					}

					_, exists := instrumentHandlerMap[deribitMsg.Params.Data.Instrument]
					if !exists {
						var instrumentHandler handler.InstrumentHandler
						instrumentCh := make(chan handler.DeribitMessage)
						instrumentHandler.Init(ctx, &wg, kafkaWriter, instrumentCh, reconnectCh)
						instrumentHandlerMap[deribitMsg.Params.Data.Instrument] = instrumentCh
						wg.Add(1)
						go func() {
							defer wg.Done()
							instrumentCh <- deribitMsg
						}()
					} else {
						instrumentCh := instrumentHandlerMap[deribitMsg.Params.Data.Instrument]
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
}
