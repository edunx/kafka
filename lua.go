package kafka

import (
	"github.com/edunx/lua"
	"github.com/spf13/cast"
)

const (
	MT string = "kafka_mt"
)

func CheckKafkaUserData(L *lua.LState , idx int) *Kafka {
	ud := L.CheckUserData(idx)
	switch k := ud.Value.(type) {
	case *Kafka:
		return k
	default:
		L.ArgError(idx ,"args " + cast.ToString(idx) + " must kafka userdata")
		return nil
	}
}

func (k *Kafka) ToUserData(L *lua.LState) *lua.LUserData {
	return L.NewUserDataByInterface(k , MT)
}

func CreateKafkaUserData(L *lua.LState) int {
	opt := L.CheckTable(1)
	kfk := &Kafka{

		C: Config{
			name: opt.CheckString("name", "null"),
			topic: opt.RawGetString("topic").String(),
			addr:  opt.CheckSocketArray(L , "addr"),
			buffer:      opt.CheckInt("buffer", 4096),
			num:         opt.CheckInt("num", 100),
			flush:       opt.CheckInt("flush", 5),
			limit:       opt.CheckInt("limit", 0),
			heartbeat:   opt.CheckInt("heartbeat", 5),
			timeout:     opt.CheckInt("timeout", 30), //30s
			compression: opt.CheckString("compression", "none"),
		},
	}

	Out.Debug("kafka connection config info is: %v", kfk.C)
	if err := kfk.Start(); err != nil {
		L.RaiseError("%s kafka start err:%v", kfk.C.name, err)
		return 0
	}

	ud := L.NewUserDataByInterface(kfk , MT)
	L.Push(ud)
	return 1
}

func LuaInjectApi(L *lua.LState, parent *lua.LTable, output Logger) {
	mt := L.NewTypeMetatable(MT)

	//获取字段
	L.SetField(mt, "__index", L.NewFunction(Get))

	//设置字段
	L.SetField(mt, "__newindex", L.NewFunction(Set))

	L.SetField(parent, "kafka", L.NewFunction(CreateKafkaUserData))

	//注入日志输出方法
	Out = output
}

func Get(L *lua.LState) int {
	self := CheckKafkaUserData(L, 1)
	name := L.CheckString(2)
	switch name {

	case "reload":
		//set reload to lua
		L.Push(L.NewFunction(func(L *lua.LState) int {
			self.Reload()
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
