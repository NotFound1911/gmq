package prometheus

import (
	"fmt"
	"github.com/NotFound1911/gmq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestMiddlewareBuilder_Build(t *testing.T) {
	build := MiddlewareBuilder{
		Name:      "test",
		Subsystem: "message",
		Namespace: "memory",
	}
	b := gmq.NewBroker()
	b.Use(build.Build())
	go func() {
		for {
			err := b.Send(
				&gmq.Msg{
					Topic:   "t1",
					Content: strconv.Itoa(rand.Intn(2)),
				})
			if err != nil {
				fmt.Println("err:", err)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	go func() {
		for {
			err := b.Send(
				&gmq.Msg{
					Topic:   "t2",
					Content: strconv.Itoa(rand.Intn(5)),
				})
			if err != nil {
				fmt.Println("err:", err)
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	go func() {
		msgs, err := b.Subscribe("t1", 100)
		if err != nil {
			t.Log(err)
			return
		}
		for msg := range msgs {
			fmt.Println("t1 ", msg.Content)
		}
	}()
	go func() {
		msgs, err := b.Subscribe("t2", 100)
		if err != nil {
			t.Log(err)
			return
		}
		for msg := range msgs {
			fmt.Println("t2 ", msg.Content)
		}
	}()
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":8082", nil)
	}()
	b.Listen()
}
