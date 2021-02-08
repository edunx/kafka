package kafka

import "context"

const (
	START      = iota
	CLOSE
	ERROR
	OK
)

type Config struct {
	name        string
	addr        string // 192.168.1.1:9092,192.168.1.2:9092
	timeout     int    // not used
	topic       string
	num         int // 每个线程每次发送的数据条数
	flush       int // 强制发送数据间隔时长
	buffer      int // 缓冲区大小
	thread      int
	limit       int
	compression string // 压缩方式, GZIP,LZ4,None,Snappy,ZSTD
	heartbeat   int    // 心跳检测周期
}

type Kafka struct {

	C Config

	send    uint64 //发送的总数
	recv    uint64 //接收的总数

	thread  []Thread
	limiter  *Limiter
	ctx     context.Context
	cancel  context.CancelFunc
	buffer  chan []byte
	close   bool
}
