package main

import (
	"log"
	"sync"
	"github.com/nsqio/go-nsq"
)

func main() {

	wg := &sync.WaitGroup{}
	wg.Add(1000) //安排1000个待执行的等待组

	config := nsq.NewConfig()
	//第一个参数是话题test，第二是通道名字，然后用AddHandler添加一个消费处理函数，在处理函数中会打印这个消息。
	q, _ := nsq.NewConsumer("test", "ch", config) //新建一个消费者
	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error { //里面会开几个协程（默认为1）来轮询这个消息管道incomingMessages，里面的消息是readLoop放进去的。readLoop在ConnectToNSQD中
		log.Printf("Got a message: %s", message.Body)
		wg.Done() //AddHandler本身是开了一个协程来处理收消息。
		return nil
	}))
	//建立多个nsqd连接c.ConnectToNSQDs([]string{"127.0.0.1:4150", "127.0.0.1:4152"})
	err := q.ConnectToNSQD("127.0.0.1:4150")
	if err != nil {
		log.Panic(err)
	}
	wg.Wait()

}
