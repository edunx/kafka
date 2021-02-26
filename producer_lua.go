package kafka

import (
	"github.com/edunx/lua"
	pub "github.com/edunx/rock-public-go"
)

const (
	PRODUCERMT string = "ROCK_KAFKA_PRODUCER_GO_MT"
)


func (p *Producer) ToUserData(L *lua.LState) *lua.LUserData {
	return L.NewUserDataByInterface(p, PRODUCERMT)
}

func CreateProducerUserData(L *lua.LState) int {
	opt := L.CheckTable(1)
	kfk := &Producer{

		C: Config{
			name: opt.CheckString("name", "null"),
			topic: opt.RawGetString("topic").String(),
			addr:        opt.CheckSockets("addr" , L),
			buffer:      opt.CheckInt("buffer", 4096),
			num:         opt.CheckInt("num", 100),
			flush:       opt.CheckInt("flush", 5),
			limit:       opt.CheckInt("limit", 0),
			heartbeat:   opt.CheckInt("heartbeat", 5),
			timeout:     opt.CheckInt("timeout", 30), //30s
			thread:     opt.CheckInt("thread", 10), //10 thread 
			compression: opt.CheckString("compression", "none"),
		},
	}

	pub.Out.Debug("kafka connection config info is: %v", kfk.C)
	if err := kfk.Start(); err != nil {
		L.RaiseError("%s kafka start err:%v", kfk.C.name, err)
		return 0
	}

	ud := L.NewUserDataByInterface(kfk , PRODUCERMT)
	L.Push(ud)
	return 1
}

func LuaInjectProducerApi(L *lua.LState, parent *lua.LTable ) {
	mt := L.NewTypeMetatable(PRODUCERMT)

	//获取字段
	L.SetField(mt, "__index", L.NewFunction(Get))

	//设置字段
	L.SetField(mt, "__newindex", L.NewFunction(Set))

	L.SetField(parent, "producer", L.NewFunction(CreateProducerUserData))
}

func Get(L *lua.LState) int {
	self := CheckProducerUserData(L, 1)
	name := L.CheckString(2)
	switch name {

	case "reload":
		//set reload to lua
		L.Push(L.NewFunction(func(L *lua.LState) int {
			self.Reload()
			return 0
		}))

	case "push":
		L.Push(L.NewFunction(func(L *lua.LState) int {
			v := L.CheckString(1)
			self.Push(v)
			return 0
		}))
	default:
		L.Push(lua.LNil)
	}
	return 1
}

func Set(L *lua.LState) int {
	return 0
}