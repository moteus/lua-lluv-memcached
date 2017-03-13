local function prequire(m)
  local ok, mod = pcall(require, m)
  if ok then return mod end
  return nil, mod
end

local function gc(cmd, ...)
  if cmd == 'collect' then
    for i = 1, 100 do collectgarbage('collect') end
  else
    return collectgarbage(cmd, ...)
  end
end

local timer_start, timer_stop do

local zmq  = prequire "lzmq"

if zmq then

function timer_start()
  local timer = zmq.utils.stopwatch():start()
  return timer
end

function timer_stop(timer)
  local elapsed = timer:stop()
  return elapsed, 1000000
end

else

function timer_start()
  return os.clock()
end

function timer_stop(timer)
  return os.clock() - timer, 1
end

end

end

local function uvmemc_perform(database_count, query_count, key, value)
  local uv        = require "lluv"
  local Memcached = require "lluv.memcached"

  database_count = database_count or 1
  local databases = {}
  for i = 1, database_count do
    databases[i] = Memcached.Connection.new()
  end

  local counter, timer = query_count
  local function execute(self, err, res)
    counter = counter - 1

    if counter == 0 then
      local elapsed, resolution = timer_stop(timer)
      local throughput = query_count / (elapsed / resolution)
      print(string.format("memc(%d) mean throughput: %.2f [qry/s]", database_count, throughput))
      execute = nil
      return uv.stop()
    end

    self:get('test_key', execute)
  end

  local n = database_count
  for i = 1, database_count do
    databases[i]:open(function(self)
      self:set(key, value, function(self, err, res)
        n = n - 1
        if n == 0 then
          timer = timer_start()
          for i = 1, database_count do
            execute(databases[i])
          end
        end
      end)
    end)
  end

  uv.run()
  for i = 1, database_count do
    databases[i]:close()
  end
  uv.run()
end

local function mmemcache_perform(query_count, key, value)
  local Memcached = prequire "memcached"
  if not Memcached then return print("lua-memcached not installed") end

  local mmc = Memcached.connect()

  mmc:set(key, value)
  local timer = timer_start()

  for i = 1, query_count do
    mmc:get(key)
  end

  local elapsed, resolution = timer_stop(timer)
  local throughput = query_count / (elapsed / resolution)
  print(string.format("memc mean throughput: %.2f [qry/s]", throughput))

  mmc:quit()
end

local query_count = 100000
local key, value = 'test_key', 'test_value'

uvmemc_perform(4, query_count, key, value)
uvmemc_perform(3, query_count, key, value)
uvmemc_perform(2, query_count, key, value)
uvmemc_perform(1, query_count, key, value)
mmemcache_perform(query_count, key, value)