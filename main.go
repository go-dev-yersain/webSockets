package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"gopkg.in/melody"
	"net/http"
	"sync"
)

type JsonResponse struct {
	Authorization string `json:"Authorization"`
	ClientId      string `json:"ClientId"`
}

var wsConnections *WsSessions
var wg *sync.WaitGroup

func main() {
	wg = new(sync.WaitGroup)
	wg.Add(3)
	kafkaMsgCh := make(chan *Message, 1)
	wsConnections = NewWsSession()
	fmt.Println(wsConnections.Size())

	go webSocketListener(wg)
	go kafkaConsumer(wg, kafkaMsgCh)
	go msgDispatcher(kafkaMsgCh)
	wg.Wait()
}

func msgDispatcher(msgCh <-chan *Message) {
	for {
		select {
		case msg := <-msgCh:
			clientId := msg.Keys.ClientId
			val, ok := wsConnections.Load(clientId)
			if !ok {
				fmt.Println("Session not found")
			} else {
				val.Write([]byte("catch"))
			}
		}
	}
}

func webSocketListener(wg *sync.WaitGroup) {
	defer wg.Done()
	r := gin.Default()
	m := melody.New()

	r.GET("/", func(c *gin.Context) {
		http.ServeFile(c.Writer, c.Request, "index.html")
	})

	r.GET("/ws", func(c *gin.Context) {
		m.HandleRequest(c.Writer, c.Request)
	})

	m.HandleMessage(func(s *melody.Session, msg []byte) {
		m := new(JsonResponse)
		json.Unmarshal(msg, m)
		wsConnections.Store(m.ClientId, s)
		fmt.Println(wsConnections.Size())
	})

	m.HandleConnect(func(s *melody.Session) {

	})

	r.Run(":5000")
}

func kafkaConsumer(wg *sync.WaitGroup, msgCh chan<- *Message) {
	defer wg.Done()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	defer c.Close()

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	c.SubscribeTopics([]string{"testTopic", "^aRegex.*[Tt]opic"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			m := new(Message)
			err := json.Unmarshal(msg.Value, &m)
			if err != nil {
				fmt.Println(err)
			} else {
				msgCh <- m
			}
			fmt.Println(m)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

}

type Message struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
	Keys  struct {
		ProcessId string `json:"process_id"`
		PublicId  string `json:"public_id"`
		ClientId  string `json:"client_id"`
		Token     string `json:"token"`
	} `json:"keys"`
}


//{"event":"push","keys":{"process_id":"p1","public_id":"pb1","client_id":"cl2","token":"lkfdnvlkdsjf"}}


//{"event":"push","keys":{"process_id":"p1","public_id":"pb1","client_id":"cl1","token":"lkfdnvlkdsjf"}}

//{"id":"numero uno","type":"transaction","data":{"id":"numero uno","type":"transaction","amount":"1000","currency":"usd"}}"`
