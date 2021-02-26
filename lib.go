package kafka

import (
	"github.com/edunx/lua"
	"github.com/spf13/cast"
)

func CheckProducerUserData(L *lua.LState , idx int) *Producer {
	ud := L.CheckUserData(idx)
	switch k := ud.Value.(type) {
	case *Producer:
		return k
	default:
		L.ArgError(idx ,"args " + cast.ToString(idx) + " must kafka userdata")
		return nil
	}
}
