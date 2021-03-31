package kafka

import (
	"github.com/edunx/lua"
)

func (p *Producer) ToLightUserData(L *lua.LState) *lua.LightUserData {
	return  &lua.LightUserData{Value:  p }
}

func (p *Producer) LStart(L *lua.LState , args *lua.Args) lua.LValue {
	err := p.Start()
	if err != nil {
		L.RaiseError("kafka.producer.%s start fail , err:%v" , p.Name() , err )
		return lua.LNil
	}
	return lua.LString("kafka.producer." + p.Name() + " start succeed")
}

func (p *Producer) LClose(L *lua.LState , args *lua.Args) lua.LValue {
	if err := p.Close(); err != nil {
		L.RaiseError("kafka.producer.%s close fail , err:%v" , p.Name() , err )
		return lua.LNil
	}

	return lua.LString("kafka.producer." + p.Name() + " close succeed")
}

func (p *Producer) LReload(L *lua.LState , args *lua.Args) lua.LValue {

	if err := p.Close(); err != nil {
		L.RaiseError("reload kafka.producer.%s close fail , err:%v" , p.Name() , err )
		return lua.LNil
	}

	if err := p.Start() ; err != nil {
		L.RaiseError("reload kafka.producer.%s start fail , err:%v" , p.Name() , err )
		return lua.LNil
	}

	return lua.LString("kafka.producer." + p.Name() + " reload succeed")
}

func (p *Producer) LWrite(L *lua.LState , args *lua.Args) lua.LValue {
	n := args.Len()
	if n <= 0 {
		return lua.LNil
	}

	for i := 1 ;i <= n ; i++ {
		v := args.CheckString(L , i)
		p.Write(v)
	}
	return lua.LNil
}

func (p *Producer) LToJson(L *lua.LState , args *lua.Args) lua.LValue {
	v , _ := p.ToJson()
	return lua.LString(v)
}

func (p *Producer) Index(L *lua.LState , key string) lua.LValue {

	if key == "start"   { return lua.NewGFunction( p.LStart )    }
	if key == "reload"  { return lua.NewGFunction( p.LReload )   }
	if key == "close"   { return lua.NewGFunction( p.LClose )    }
	if key == "write"   { return lua.NewGFunction( p.LWrite )    }
	if key == "json"    { return lua.NewGFunction( p.LToJson)    }

	return lua.LNil
}

func createProducerUserData(L *lua.LState , args *lua.Args) lua.LValue {
	opt := args.CheckTable(L , 1)
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

	ud := &lua.LightUserData{Value: kfk}
	return ud
}