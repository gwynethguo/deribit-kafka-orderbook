package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Checker struct {
	wg *sync.WaitGroup
}

func (c *Checker) Init(ctx context.Context, wg *sync.WaitGroup) {
	c.wg = wg

	c.wg.Add(1)
	go c.start(ctx)
}

func (c *Checker) handleInstrumentCheck(instrumentCh <-chan DeribitMessage, ctx context.Context) {
	defer c.wg.Done()

	var prevMsg DeribitMessage
	nextIsSnapshot := true

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-instrumentCh:
			if msg.Params.Data.Timestamp < prevMsg.Params.Data.Timestamp {
				log.Fatalln("Messages have been reordered!")
			}
			if nextIsSnapshot && (msg.Params.Data.Type != "snapshot") {
				log.Fatalln("Snapshot not captured when message lost is detected!")
			}
			nextIsSnapshot = false
			if (msg.Params.Data.PrevChangeId != nil) && (*msg.Params.Data.PrevChangeId != prevMsg.Params.Data.ChangeId) {
				nextIsSnapshot = true
			}
			prevMsg = msg
		}
	}
}

func (c *Checker) start(ctx context.Context) {
	defer c.wg.Done()
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"localhost:9092"}, Topic: kafkaTopic})

	instruments := make(map[string]chan DeribitMessage)

	for {
		select {
		case <-ctx.Done():
			for _, ch := range instruments {
				close(ch)
			}
			return
		default:
			msg, err := kafkaReader.ReadMessage(ctx)
			if err != nil {
				log.Fatalf("Error reading message: %v\n", err)
			}

			log.Printf("[KAFKA CONSUMER] KEY: %s, VALUE: %s\n", string(msg.Key), string(msg.Value))

			var deribitMsg DeribitMessage
			if err := json.Unmarshal(msg.Value, &deribitMsg); err != nil {
				log.Printf("Failed to unmarshal message: %v\n", err)
				continue
			}

			if deribitMsg.Method != "subscription" {
				continue // Skip non-orderbook messages
			}

			if string(msg.Key) != deribitMsg.Params.Data.Instrument {
				log.Fatalf("Instrument in Key %s not equal to instrument in Value %s\n", string(msg.Key), deribitMsg.Params.Data.Instrument)
			}

			_, exists := instruments[deribitMsg.Params.Data.Instrument]
			if !exists {
				instrumentCh := make(chan DeribitMessage)
				instruments[deribitMsg.Params.Data.Instrument] = instrumentCh
				c.wg.Add(2)
				go c.handleInstrumentCheck(instrumentCh, ctx)
				go func() {
					defer c.wg.Done()
					instrumentCh <- deribitMsg
				}()
			} else {
				instrumentCh := instruments[deribitMsg.Params.Data.Instrument]
				c.wg.Add(1)
				go func() {
					defer c.wg.Done()
					instrumentCh <- deribitMsg
				}()
			}
		}
	}
}
