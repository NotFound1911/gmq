package gmq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBroker_Send(t *testing.T) {
	b := &MessageQueue{
		chans: map[string][]chan *Msg{},
	}
	go func() {
		for {
			err := b.Send(
				&Msg{
					Topic:   "t1",
					Content: time.Now().String(),
				})
			if err != nil {
				t.Log(err)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()
	go func() {
		for {
			err := b.Send(
				&Msg{
					Topic:   "t2",
					Content: time.Now().String(),
				})
			if err != nil {
				t.Log(err)
				return
			}
			time.Sleep(time.Second)
		}

	}()
	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		var topic = "t1"
		if i%2 == 0 {
			topic = "t2"
		}
		name := fmt.Sprintf("topic:%v consumer:%v", topic, i)
		go func(topic, name string) {
			defer wg.Done()
			msgs, err := b.Subscribe(topic, 100)
			if err != nil {
				t.Log(err)
				return
			}

			for msg := range msgs {
				fmt.Println(name, msg.Content)
			}
		}(topic, name)
	}
	wg.Wait()
}
