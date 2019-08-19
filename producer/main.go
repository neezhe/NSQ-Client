package main

import (
	"github.com/nsqio/go-nsq"
	"log"
	"math/rand"
	"time"
)

func main() {
	config := nsq.NewConfig()
	w, err := nsq.NewProducer("192.168.0.107:4150", config) //第一个参数就是nsqd的地址

	if err != nil {
		log.Panic(err)
	}

	chars := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	for { //在这里做了个无限for循环，每次随机4个byte发布到test话题里面。
		buf := make([]byte, 6)
		for i := 0; i < 6; i++ {
			buf[i] = chars[rand.Intn(len(chars))]
		}
		log.Printf("Pub: %s", buf)
		err = w.Publish("lizhe", buf)
		if err != nil {
			log.Panic(err)
		}
		time.Sleep(time.Second * 1)

		break
	}

	w.Stop()
}
