package main

import (
	"log"
	"fmt"
	"github.com/nsqio/go-nsq"
)

func doSimpleConsumerTask() {
	// 1. 创建消费者
	config := nsq.NewConfig()
	//第一个参数是话题test，第二是通道名字，然后用AddHandler添加一个消费处理函数，在处理函数中会打印这个消息。
	q, errNewCsmr := nsq.NewConsumer("lizhe", "ch-a-test", config) //新建一个消费者
	if errNewCsmr != nil {
		fmt.Printf("fail to new consumer!, topic=%s, channel=%s", "a-test", "ch-a-test")
	}

	// 2. 添加处理消息的方法
	//里面会开几个协程（默认为1）来轮询这个消息管道incomingMessages，里面的消息是readLoop放进去的。readLoop在ConnectToNSQD中
	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		log.Printf("message: %v", string(message.Body))
		message.Finish()
		return nil
	}))

	// 3. 通过http请求来发现nsqd生产者和配置的topic（推荐使用这种方式）
	lookupAddr := []string{
		"127.0.0.1:4161",
	}
	err := q.ConnectToNSQLookupds(lookupAddr)
	if err != nil {
		log.Panic("[ConnectToNSQLookupds] Could not find nsqd!")
	}
	//如果本身知道nsqd的地址，也可不通过Lookupds来查找
	//	err := q.ConnectToNSQD("127.0.0.1:4150")
	//	if err != nil {
	//		log.Panic(err)
	//	}

	// 4. 接收消费者停止通知
	<-q.StopChan

	// 5. 获取统计结果
	stats := q.Stats()
	fmt.Sprintf("message received %d, finished %d, requeued:%s, connections:%s",
		stats.MessagesReceived, stats.MessagesFinished, stats.MessagesRequeued, stats.Connections)
}

func main() {
	doSimpleConsumerTask()
}
