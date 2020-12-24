--
-- Created by IntelliJ IDEA.
-- User: root
-- Date: 2020/10/13
-- Time: 下午12:43
-- To change this template use File | Settings | File Templates.
-- 配置使用说明
--

local kfk = rock.kafka{
    addr = "127.0.0.1:9092,127.0.0.2:9092",
    timeout = 30,
    topic = "test",
    num = 10,
    interval = 30,
    thread = 5,
    limit = 10000,
    compression = "gzip",
    heartbeat = 10,
}

--print(kfk.start())
