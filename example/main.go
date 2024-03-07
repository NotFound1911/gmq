package main

import (
	"fmt"
	"github.com/NotFound1911/gmq"
	"time"
)

func main() {
	b := gmq.NewBroker()
	go func() {
		for {
			err := b.Send(
				&gmq.Msg{
					Topic:   "t1",
					Content: time.Now().String(),
				})
			if err != nil {
				fmt.Println("err:", err)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()
	b.Listen()
}
