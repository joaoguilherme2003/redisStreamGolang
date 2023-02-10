package prod

import (
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/vmihailenco/msgpack/v4"
)

type SomeEvent struct {
	Id        uint
	Descricao string
	Horario   time.Time
}

var client *redis.Client
var streamName string = os.Getenv("STREAM")

func init() {
	var err error
	client, err = newRedisClient()
	if err != nil {
		panic(err)
	}
}

func main() {

	event1 := SomeEvent{
		Id:        1,
		Descricao: "something",
		Horario:   time.Now(),
	}

	event2 := SomeEvent{
		Id:        2,
		Descricao: "something2",
		Horario:   time.Now(),
	}

	event3 := SomeEvent{
		Id:        3,
		Descricao: "something3",
		Horario:   time.Now(),
	}

	produceMsg(map[string]interface{}{
		"type": "someEvent",
		"data": &event1,
	})

	time.Sleep(time.Second * 3)
	produceMsg(map[string]interface{}{
		"type": "someEvent",
		"data": &event2,
	})

	time.Sleep(time.Second * 5)
	produceMsg(map[string]interface{}{
		"type": "someEvent",
		"data": &event3,
	})

}

func produceMsg(pessoa map[string]interface{}) {
	client.XAdd(&redis.XAddArgs{
		Stream: streamName,
		Values: pessoa,
	})
}

func newRedisClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:6379", os.Getenv("REDIS_HOST")),
		Password: "",
		DB:       0,
	})

	_, err := client.Ping().Result()
	return client, err

}

func (o SomeEvent) UnmarshalBinary(data []byte) error {
	return msgpack.Unmarshal(data, o)
}
