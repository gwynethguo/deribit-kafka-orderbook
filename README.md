# Deribit Kafka Order Book

This project connects to the Deribit WebSocket API to subscribe to order book updates for all BTC options available through Deribit, processes the data, and publishes it to a Kafka topic. It also includes a Kafka consumer to validate the integrity of the order book messages.

## Table of Contents

- [Environment Setup](#environment-setup)
- [Build Instructions](#build-instructions)
- [Run Instructions](#run-instructions)
- [Thought Process](#thought-process)
- [Scenarios Considered](#scenarios-considered)

---

## Environment Setup

1. **Install Go**  
   Ensure you have Go installed on your system. This project requires Go version `1.24.2` or later. You can download it from [Go's official website](https://golang.org/dl/).

2. **Install Docker**  
   Kafka is set up using Docker. Install Docker from [Docker's official website](https://www.docker.com/).

3. **Clone the Repository**  
   Clone this repository to your local machine:

   ```sh
   git clone https://github.com/gwynethguo/deribit-kafka-orderbook.git
   cd deribit-kafka-orderbook
   ```

4. **Install Dependencies**  
   Run the following command to install the required Go modules:
   ```sh
   go mod tidy
   ```

---

## Build Instructions

1. **Build the Project**  
   Use the `go build` command to compile the project:
   ```sh
   go build -o deribit-kafka-orderbook main.go
   ```

---

## Run Instructions

1. **Start Kafka**  
   Use the provided `docker-compose.yml` file to start a Kafka broker:

   ```sh
   docker-compose up -d
   ```

2. **Create the Topic**  
   Use the `kafka-topics.sh` script to create a topic. Replace `<topic-name>` with `deribit_orderbook`, and adjust the replication factor and partitions as needed:

   ```sh
   kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic orderbook-updates
   ```

3. **Verify the Topic**  
   To confirm that the topic was created successfully, list all topics:

   ```sh
   kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

   You should see your topic (`deribit_orderbook` in the example above) in the list

   Notes

   - If you're using Docker, you may need to run the commands inside the Kafka container. For example:

   ````sh
   docker exec -it <kafka-container-name> kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic <topic-name>```
   ````

- Replace `<kafka-container-name>` with the name of your Kafka container.

4. **Run the Application**  
   Execute the compiled binary:

   ```sh
   ./deribit-kafka-orderbook
   ```

   Alternatively, you can run the project directly using:

   ```sh
   go run main.go
   ```

5. **Run the Checker**  
   The checker validates the integrity of the order book messages. Currently, it can't read only the new messages, so it reads messages from the beginning. Run it using:
   ```sh
   go run checker/checker.go
   ```

---

## Thought Process

My first approach was to use one WebSocket for each instrument. However, looking at the possibility of having a large number of instruments for BTC/USD options (1000+ instruments), I changed my implementation to use one WebSocket instead. I also thought that having only one connection might make the program slower. However, I also noticed that the changes for each instrument (different expiry date, price strike, and type) doesn't happen so frequently. Therefore, using a few WebSockets should be able to produce the order book updates into Kafka in real-time to be used for market making, while also keeping the updates available for backtesting trading strategies.

1. **WebSocket Subscription**  
   The application subscribes to Deribit order book updates in batches of 50 instruments. This ensures efficient subscription without overwhelming the WebSocket connection.

2. **Kafka Integration**  
   The application uses Kafka to publish order book updates. Each message is keyed by the instrument name, ensuring that messages for the same instrument are grouped together.

3. **Error Handling**

   - WebSocket errors are logged, and reconnections are handled gracefully.
   - Kafka errors are logged.

4. **Concurrency**

   - Goroutines are used to handle multiple instruments concurrently.
   - A `sync.WaitGroup` ensures proper synchronization and cleanup of resources.
   - Channels are used to synchronise between goroutines.

5. **Logging**  
   Logs are written to files for debugging and monitoring purposes. The `.log` files are ignored in version control using the .gitignore file.

---

## Scenarios Considered

1. **WebSocket Disconnection**  
   If the WebSocket connection drops, the application reconnects and resubscribes to all instruments.

2. **Message Loss**  
   The `PrevChangeId` field in the order book messages is used to detect message loss. If a gap is detected, the application resubscribes to the affected instrument.

3. **Kafka Broker Unavailability**  
   If the Kafka broker is unavailable, the application logs the error. Retrying is currently not implemented. However, it will be considered for future improvements.

4. **Invalid API Responses**  
   The application validates API responses and logs any errors. For example, if the Deribit API returns an error code, the application logs the error and exits.

5. **High Volume of Instruments**  
   The application processes instruments in batches to handle a large number of instruments efficiently.
