package gmq

import (
	"errors"
	"sync"
)

type MessageQueue struct {
	mutex  sync.RWMutex
	chans  map[string][]chan *Msg
	reject bool
}
type Msg struct {
	Topic   string
	Content string
}

func (b *MessageQueue) Send(m *Msg) error {
	if b.reject {
		return errors.New("消息队列准备关闭, 拒绝发送")
	}
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	for _, ch := range b.chans[m.Topic] {
		select {
		case ch <- m:
		default:
			return errors.New("消息队列已满")
		}
	}
	return nil
}
func (b *MessageQueue) Subscribe(topic string, capacity int) (<-chan *Msg, error) {
	res := make(chan *Msg, capacity)
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, ok := b.chans[topic]; !ok {
		b.chans[topic] = []chan *Msg{res}
		return res, nil
	}
	b.chans[topic] = append(b.chans[topic], res)
	return res, nil
}
func (b *MessageQueue) Close() error {
	b.mutex.Lock()
	chans := b.chans
	b.chans = nil
	b.mutex.Unlock()

	// 避免了重复 close chan 的问题
	for _, chs := range chans {
		for _, ch := range chs {
			close(ch)
		}
	}
	return nil
}
func (b *MessageQueue) rejectSend() {
	b.reject = true
}
