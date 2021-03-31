package kafka

import (
	"context"
	"fmt"
	"github.com/edunx/lua"
	pub "github.com/edunx/rock-public-go"
	tp "github.com/edunx/rock-transport-go"
	"golang.org/x/time/rate"
	"sync/atomic"
	"time"
)

//name        string
//addr        string // 192.168.1.1:9092,192.168.1.2:9092
//timeout     int    // not used
//topic       string
//num         int // 每个线程每次发送的数据条数
//flush       int // 强制发送数据间隔时长
//buffer      int // 缓冲区大小
//thread      int
//limit       int
//compression string // 压缩方式, GZIP,LZ4,None,Snappy,ZSTD
//heartbeat   int    // 心跳检测周期

func (p *Producer) NewConfig( name , addr , topic , compression , log string ,
	timeout , num , flush , buffer , thread , limit , heartbeat ,level int ) {
	pub.SetOutput( log , level )

	p.C = Config{
		name: name ,
		addr: addr ,
		topic: topic ,
		compression: compression,
		timeout: timeout,
		num:num,
		flush: flush,
		buffer: buffer,
		thread: thread,
		limit:limit,
		heartbeat: heartbeat,
	}
}

func (p *Producer) Write(v interface{} ) error {
	p.Push(v)
	return nil
}

func (p *Producer) Push( v interface{} ) {
	if p.state != lua.RUNNING {
		time.Sleep(time.Second)
	}

	var data []byte
	switch msg := v.(type) {
	case string:
		data = []byte(msg)
	case []byte:
		data = msg
	case tp.Message: //消耗挺大 不建议
		data = msg.Byte()
	default:
        data = []byte(fmt.Sprintf("%v" , msg))
	}

	atomic.AddUint64(&p.recv , uint64(len(data)))
	p.buffer <-data
}

// 开始传输
func (p *Producer) Start() error {

	p.thread = make([]Thread, p.C.thread)
	p.buffer = make(chan []byte , p.C.buffer)
	p.ctx , p.cancel = context.WithCancel(context.Background())
	p.limiter = &Limiter{}

	//设置限速
	if p.C.limit > 0 {
		p.limiter.limit = rate.NewLimiter(rate.Limit(p.C.limit) , p.C.limit * 2)
		p.limiter.ctx , _ = context.WithCancel(context.TODO())
	} else {
		p.limiter.limit = nil
	}

	//创建并启动程序
	for i := 0; i < p.C.thread; i++ {

		p.thread[i] = NewThread(i , p)
		go p.thread[i].start() //启动线程
	}

	go p.Heartbeat()

	return nil
}

// 线程状态检测
func (p *Producer) State() bool {
	inactive := 0

	for _ , v := range p.thread {
		if v.status != OK {
			inactive++
		}
	}

	if inactive == p.C.thread {
		return false
	}

	return  true
}

func (p *Producer) Ping() {

	for id, t := range p.thread {
		switch t.status {
		case OK:
			//pub.Out.Info("%s kafka thread.id = %d up" , t.C.name , id)
			continue

		case CLOSE:
			pub.Out.Info("%s kafka thread.id = %d close" , t.C.name , id)
			//pub.Out.Err("%s kafka threads check: topic [%s], %d up, %d down", p.C.name , p.C.topic, p.count, p.C.thread-p.count)
		case ERROR:
			go p.thread[id].start()
			//pub.Out.Err("%s kafka thread.id = %d start" , p.C.name , id)
		}
	}

}

// 心跳检测
func (p *Producer) Heartbeat() {
	tk := time.NewTicker( time.Second * time.Duration(p.C.heartbeat))
	defer tk.Stop()

	for {
		select {
		case <-p.ctx.Done():
			pub.Out.Err("%s kafka heartbeat exit", p.C.name)
			return
		case <-tk.C:
			p.Ping()
		}
	}
}

// 关闭连接
func (p *Producer) Close() error {
	p.cancel()
	p.state = lua.CLOSE
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (p *Producer) Reload() {
	pub.Out.Err("%s kafka reload to close ..." , p.C.name)
	p.Close()
	pub.Out.Err("%s kafka reload  close end" , p.C.name)

	if err := p.Start() ; err != nil {
		pub.Out.Err("%s kafka reload , start err: %v" , p.C.name , err)
	}
}

func (p *Producer) Type() string {
	return "kafka"
}

func (p *Producer) Name() string {
	return p.C.name
}

func (p *Producer) ToJson() ( []byte , error ) {
	buff := lua.NewJsonBuffer()
	buff.Start("kafka.producer")
	buff.WriteKV("name" , p.C.name , false)
	buff.WriteKV("addr" , p.C.addr, false)
	buff.WriteKV( "topic" , p.C.topic , false)
	buff.WriteKI( "num" , p.C.num, false)
	buff.WriteKI( "flush" , p.C.flush, false)
	buff.WriteKI( "thread" , p.C.thread, false)
	buff.WriteKI( "limit" , p.C.limit, false)
	buff.WriteKI( "heartbeat" , p.C.heartbeat, false)
	buff.WriteKI("timeout" , p.C.timeout, false)
	buff.WriteKV("compression" , p.C.compression , true)
	buff.End()
	return buff.Bytes() , nil
}

func (p *Producer) Status() (string , error) {
	return fmt.Sprintf("name:%s , status:%s , uptime:%s , recv:%d , send:%d",
		p.Name() , p.state.String() , p.uptime , p.recv, p.send) , nil
}