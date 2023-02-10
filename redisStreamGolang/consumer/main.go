package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	prod "redisstreamgolang/producer"
	"strings"
	"syscall"

	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
)

var (
	streamName    string = os.Getenv("STREAM")
	consumerGroup string = os.Getenv("GROUP")
	consumerName  string = uuid.NewV4().String()
	client        *redis.Client
)

func init() {
	var err error
	client, err = newRedisClient()
	if err != nil {
		panic(err)
	}

	createConsumerGroup()
}

func main() {

	consumeEvents()
	// desconectar
	chanOS := make(chan os.Signal)
	signal.Notify(chanOS, syscall.SIGINT, syscall.SIGTERM)
	<-chanOS
	client.Close()
}

func newRedisClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:6379", os.Getenv("REDIS_HOST")),
		Password: "",
		DB:       0, // use default DB
	})

	_, err := client.Ping().Result()
	return client, err

}

func createConsumerGroup() {
	if _, err := client.XGroupCreateMkStream(streamName, consumerGroup, "0").Result(); err != nil {

		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			fmt.Printf("Error on create Consumer Group: %v ...\n", consumerGroup)
			panic(err)
		}

	}
}

func consumeEvents() {
	for {
		func() {
			streams, err := client.XReadGroup(&redis.XReadGroupArgs{
				Streams:  []string{streamName, ">"},
				Group:    consumerGroup,
				Consumer: consumerName,
				Count:    10,
				Block:    0,
			}).Result()

			if err != nil {
				log.Printf("err on consume events: %+v\n", err)
				return
			}

			for _, stream := range streams[0].Messages {
				processStream(stream)
			}
		}()
	}
}

func processStream(stream redis.XMessage) {
	var newEvent prod.SomeEvent
	err := newEvent.UnmarshalBinary([]byte(stream.Values["data"].(string)))
	if err != nil {
		panic(err)
	}
	err = handleEvent(newEvent)
	if err != nil {
		panic(err)
	}
	client.XAck(streamName, consumerGroup, stream.ID)
}
