package gmq

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var signals = []os.Signal{
	os.Interrupt, os.Kill, syscall.SIGKILL, syscall.SIGSTOP,
	syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGILL, syscall.SIGTRAP,
	syscall.SIGABRT, syscall.SIGSYS, syscall.SIGTERM,
}

type ShutdownCallback func(ctx context.Context)

type BrokerOption func(*Broker)

func WithShutdownCallbacks(cbs ...ShutdownCallback) BrokerOption {
	return func(b *Broker) {
		b.cbs = cbs
	}
}

// Broker 实现优雅退出
type Broker struct {
	mq MessageQueue
	// 优雅退出整个超时时间，默认30秒
	shutdownTimeout time.Duration
	//  消费者时间
	customerTimeout time.Duration
	// 自定义回调超时时间，默认三秒钟
	cbTimeout time.Duration

	cbs         []ShutdownCallback
	middlewares []Middleware
}

func NewBroker() *Broker {
	return &Broker{
		mq: MessageQueue{
			chans: map[string][]chan *Msg{},
		},
		middlewares:     []Middleware{},
		cbTimeout:       3 * time.Second,
		shutdownTimeout: 30 * time.Second,
		customerTimeout: 10 * time.Second,
		cbs:             make([]ShutdownCallback, 0),
	}
}
func (b *Broker) Use(middlewares ...Middleware) {
	if b.middlewares == nil {
		b.middlewares = middlewares
		return
	}
	b.middlewares = append(b.middlewares, middlewares...)
}

// Listen 监听信号
func (b *Broker) Listen() {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, signals...)
	sig := <-ch // 第一个信号
	log.Println("listen first signal:", sig)
	go func() {
		select {
		case <-ch:
			log.Println("force exit!")
			os.Exit(1)
		case <-time.After(b.shutdownTimeout):
			log.Println("timeout, force exit!")
			os.Exit(1)
		}
	}()
	b.shutdown()
}

func (b *Broker) shutdown() {
	log.Println("start stop send...")
	b.mq.rejectSend()
	log.Println("waiting for consumers...")
	time.Sleep(b.customerTimeout)
	log.Println("start executing custom shutdown callback...")
	var wg sync.WaitGroup
	wg.Add(len(b.cbs))
	for _, cb := range b.cbs {
		c := cb
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), b.cbTimeout)
			c(ctx)
			cancel()
			wg.Done()
		}()
	}
	wg.Wait()
	log.Println("release resources...")
	b.mq.Close()
}

func (b *Broker) Send(m *Msg) error {
	root := b.mq.Send
	for i := len(b.middlewares) - 1; i >= 0; i-- {
		root = b.middlewares[i](root)
	}
	return root(m)
}
func (b *Broker) Close() {
	b.mq.Close()
}
func (b *Broker) Subscribe(topic string, capacity int) (<-chan *Msg, error) {
	return b.mq.Subscribe(topic, capacity)
}
