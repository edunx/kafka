package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"strings"
	"sync/atomic"
	"time"
    pub "github.com/edunx/public"
)

// kafka 线程
type Thread struct {
	C 				Config //配置文件

	id     			int   // 线程id
	status 			int   //状态

	producer        sarama.SyncProducer //生产者
	count           uint32  // 线程中待发送数据条数
	Messages        []*sarama.ProducerMessage //缓存当前消息
	buffer          chan []byte
	ctx             context.Context
	limiter         *Limiter  //限速器
	total           *uint64 //kafka total 指针
}

func NewThread( idx int , k *Kafka ) Thread {
	thread := Thread{
		C: k.C,
		id: idx ,
		count: 0,
	 	Messages: make([]*sarama.ProducerMessage , k.C.num),
	 	ctx: k.ctx,
	 	limiter: k.limiter,
		buffer: k.buffer ,
		total:  &k.send,
	 }

	 for i := 0; i< thread.C.num; i++ {
		thread.Messages[i] = &sarama.ProducerMessage{ Topic: thread.C.topic  }
	 }

	 return thread
}

func (t *Thread) Async( data []*sarama.ProducerMessage , size uint32) {
	if size <= 0 {
		return
	}

	var err error
	var i uint32
	var msg *sarama.ProducerMessage

	for i = 0 ; i < size ; i++ {
		msg = t.Messages[i]
		_ , _ , err = t.producer.SendMessage( msg )
		if err != nil {
			pub.Out.Err("%s kafka thread.id=%d send message err:%v" , t.C.num , t.id , err)
		} else {
			atomic.AddUint64(t.total ,1)
		}

		msg.Value = nil
	}
}

func (t *Thread) SendMessage() bool { //定时发送消息
	if t.count <= 0 {
		return true
	}

	var i uint32
	var err error
	var msg  *sarama.ProducerMessage

	rc := true
	for i = 0 ; i < t.count ; i++ {
		msg = t.Messages[i]
		_ , _ , err = t.producer.SendMessage( msg )
		if err != nil {
			pub.Out.Err("%s kafka thread.id=%d send message err:%v" , t.C.num , t.id , err)
			rc = false
		} else {
			atomic.AddUint64(t.total ,1)
		}

		msg.Value = nil
	}

	t.count = 0
	//pub.Out.Debug("%s kafka thread id  %d send data success, length is %v, total is %v",t.C.name , t.id, i , *t.total)

	return rc
}

func (t *Thread) Handler(ctx context.Context) {
	var line []byte
	var msg  *sarama.ProducerMessage

	tk := time.NewTicker( time.Second * time.Duration(t.C.flush))
	for {
		//获取当前限速令牌
		t.limiter.Handler(t.C.name , t.id)
		select {

		case <-ctx.Done():
			t.SendMessage() //保证最后缓存数据不丢
			pub.Out.Err("%s kafka thread.id=%d exit successful" ,  t.C.name , t.id)
			t.close()
			return

		//读取缓存区
		case line = <-t.buffer:

			msg = t.Messages[t.count]
			msg.Value = sarama.ByteEncoder(line)

			t.Messages[t.count] = msg
			t.count++
			if t.count == uint32(t.C.num) {
				goto Send
			}

		//定时任务触发
		case <-tk.C:
			goto Send
		}

		//没有出发任何事件
		continue

		//发送数据
	Send:
		if !t.SendMessage( ) {
			t.status = ERROR //返回error heartbeat 重启
			return
		}
	}
}

func (t *Thread) close() {

	//关闭client
	if err := t.producer.Close(); err != nil {
		pub.Out.Err("%s thread.id=%d close error: %v", t.C.name , t.id , err)
	} else {
		pub.Out.Err("%s thread.id=%d close successful", t.C.name , t.id)
	}

	t.status = CLOSE
}

func (t *Thread) start() error {

	var err error
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	//GZIP,LZ4,None,Snappy,ZSTD
	switch t.C.compression {
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	t.producer, err = sarama.NewSyncProducer(strings.Split(t.C.addr , ","), config)
	if err != nil {
		pub.Out.Debug("%s kafka thread.id=%d create client error:%v",t.C.name, t.id, err)
		return err
	}

	//开启处理缓存的handler
	go t.Handler(t.ctx)
	t.status = OK

	pub.Out.Err("%s kafka thread.id = %d start ok" , t.C.name , t.id)
	return nil
}
