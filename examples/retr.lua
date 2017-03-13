local uv        = require "lluv"
local Memcached = require "lluv.memcached"

local mc = Memcached.Connection.new{
  server = "127.0.0.1:11211";
  reconnect = 1;
}

mc:open()

mc:set('test_key', 'test_value', 60)

uv.timer():start(1000, function(timer)
  mc:get('test_key', function(self, err, value)
    timer:again(1000)
    print(err, value)
  end)
end)

uv.run()