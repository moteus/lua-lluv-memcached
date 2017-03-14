------------------------------------------------------------------
--
--  Author: Alexey Melnichuk <alexeymelnichuck@gmail.com>
--
--  Copyright (c) 2014-2017 Alexey Melnichuk <alexeymelnichuck@gmail.com>
--
--  Licensed according to the included 'LICENSE' document
--
--  This file is part of lua-lluv-memcached library.
--
------------------------------------------------------------------
-- Memcached Client
------------------------------------------------------------------

--[[-- usage
local umc = mc.Connection.new("127.0.0.1:11211")

umc:open(function(err)
  if err then return print(err) end

  for i = 1, 10 do
    umc:set("test_key", "test_value " .. i, 10, function(err, result)
      print("Set #" .. i, err, result)
    end)

    umc:get("test_key", function(err, value)
      print("Get #" .. i, err, value)
    end)
  end

  umc:get("test_key", function(err, value) umc:close() end)
end)

uv.run(debug.traceback)
]]

local _NAME      = "lluv-memcached"
local _VERSION   = "0.1.0-dev"
local _COPYRIGHT = "Copyright (c) 2014-2017 Alexey Melnichuk"
local _LICENSE   = "MIT";

local uv = require "lluv"
local ut = require "lluv.utils"
local va = require "vararg"
local EventEmitter = require "EventEmitter"

local EOL = "\r\n"
local WAIT = {}

local REQ_STORE      = 0 -- single line response
local REQ_RETR       = 1 -- line + data response
local REQ_RETR_MULTI = 2 -- line + data response (many keys)
local REQ_STAT       = 3 -- 

local SERVER_ERRORS = {
  ERROR        = true;
  CLIENT_ERROR = true;
  SERVER_ERROR = true
}

local STORE_RESP = {
  STORED        = true;
  DELETED       = true;
  NOT_STORED    = true;
  EXISTS        = true;
  NOT_EXISTS    = true;
}

local class       = ut.class
local usplit      = ut.usplit
local split_first = ut.split_first

local Error = ut.Errors("MEMCACHED", {
  { EPROTO           = "Protocol error"                 },
  { ERROR            = "Unsupported command name"       },
  { CLIENT_ERROR     = "Invalid command arguments"      },
  { SERVER_ERROR     = "Server error"                   },
  { ECONN            = "Problem with server connection" },
  { EQUEUE           = "Command queue overflow"         },
})

local function cb_args(...)
  local n = select("#", ...)
  if n > 0 then
    local cb = select(-1, ...)
    if type(cb) == 'function' then
      return cb, va.remove(n, ...)
    end
  end
  return nil, ...
end

local function ocall(fn, ...) if fn then return fn(...) end end

local function make_store(cmd, key, data, exptime, flags, noreply, cas)
  assert(cmd)
  assert(key)
  assert(data)

  if type(data) == "number" then data = tostring(data) end

  exptime = exptime or 0
  noreply = noreply or false
  flags   = flags   or 0

  local buf = { cmd, key, flags or 0, exptime or 0, #data, cas}

  if noreply then buf[#buf + 1] = "noreply" end

  -- avoid concat `data` because it could be very big
  return {table.concat(buf, " ") .. EOL, data, EOL}
end

local function make_retr(cmd, key)
  assert(cmd)
  assert(key)
  return cmd .. " " .. key .. EOL
end

local function make_change(cmd, key, noreply)
  assert(cmd)
  assert(key)
  return cmd .. " " .. key .. (noreply and " noreply" or "") .. EOL
end

local function make_inc(cmd, key, value, noreply)
  assert(cmd)
  assert(key)
  assert(value)
  return cmd .. " " .. key .. " " .. value .. (noreply and " noreply" or "") .. EOL
end

local function split_host(server, def_host, def_port)
  if not server then return def_host, def_port end
  local host, port = string.match(server, "^(.-):(%d*)$")
  if not host then return server, def_port end
  if #port == 0 then port = def_port end
  return host, port
end

local function call_q(q, ...)
  while true do
    local cb = q:pop()
    if not cb then break end
    cb(...)
  end
end

local EOF      = uv.error("LIBUV", uv.EOF)
local ENOTCONN = uv.error("LIBUV", uv.ENOTCONN)

-------------------------------------------------------------------
local MMCStream = ut.class() do

function MMCStream:__init(_self)
  self._buffer = ut.Buffer.new(EOL)
  self._queue  = ut.Queue.new()
  self._self   = _self or self -- first arg for callbacks

  return self
end

function MMCStream:execute()
  local req = self._queue:peek()

  if not req then -- unexpected reply
    local err = Error("EPROTO", data)
    return self:halt(err)
  end

  while req do
    if req.type == REQ_STORE then
      local ret = self:_on_store(req)
      if ret == WAIT then return end
      if ret == WAIT then return end
    elseif req.type == REQ_RETR then
      local ret = self:_on_retr(req)
      if ret == WAIT then return end
    elseif req.type == REQ_STAT then
      local ret = self:_on_stat(req)
    else
      assert(false, "unknown request type :" .. tostring(req.type))
    end

    req = self._queue:peek()
  end
end

function MMCStream:_on_store(req)
  local line = self._buffer:read_line()
  if not line then return WAIT end

  assert(self._queue:pop() == req)

  if STORE_RESP[line] then
    return ocall(req.cb, self._self, nil, line)
  end

  local res, value = split_first(line, " ", true)
  if SERVER_ERRORS[res] then
    return ocall(req.cb, self._self, Error(res, value))
  end

  -- for increment/decrement line is just data
  return ocall(req.cb, self._self, nil, line)
end

function MMCStream:_on_retr(req)
  if not req.len then -- we wait next value
    local line = self._buffer:read_line()
    if not line then return WAIT end

    if line == "END" then -- no more data
      assert(self._queue:pop() == req)

      if req.multi then   return ocall(req.cb, self._self, nil, req.res) end
      if not req.res then return ocall(req.cb, self._self, nil, nil) end
      return ocall(req.cb, self._self, nil, req.res[1].data, req.res[1].flags, req.res[1].cas)
    end

    local res, key, flags, len, cas = usplit(line, " ", true)
    if res == "VALUE" then
      req.key   = key
      req.len   = tonumber(len) + #EOL
      req.flags = tonumber(flags) or 0
      req.cas   = cas ~= "" and cas or nil
    elseif SERVER_ERRORS[res] then
      assert(self._queue:pop() == req)
      return ocall(req.cb, self._self, Error(res, line))
    else
      local err = Error("EPROTO", line)
      return self:halt(err)
    end
  end

  assert(req.len)

  local data = self._buffer:read_n(req.len)
  if not data then return WAIT end
  
  if not req.res then req.res = {} end
  req.res[#req.res + 1] = { data = string.sub(data, 1, -3); flags = req.flags; cas = req.cas; key = req.key; }
  req.len = nil
end

function MMCStream:_on_stat(req)
  local line = self._buffer:read_line()
  if not line then return WAIT end

  if line == "END" then -- no more data
    assert(self._queue:pop() == req)
    return ocall(req.cb, self._self, nil, req.res)
  end

  local res, data = ut.split_first(line, ' ', true)
  if res == 'STAT' then
    local k, v = ut.split_first(data, ' ', true)
    if not req.res then req.res = {} end
    req.res[k] = v
  elseif SERVER_ERRORS[res] then
    assert(self._queue:pop() == req)
    return ocall(req.cb, self._self, Error(res, line), req.res)
  else
    local err = Error("EPROTO", line)
    return self:halt(err)
  end
end

function MMCStream:append(data)
  self._buffer:append(data)
  return self
end

function MMCStream:request(data, type, cb)
  if not self._on_request(self._self, data, cb) then
    return
  end

  local req
  if type == REQ_RETR_MULTI then
    req = {type = REQ_RETR, cb=cb, multi = true}
  else
    req = {type = type, cb=cb}
  end

  self._queue:push(req)

  return self._self
end

function MMCStream:on_request(handler)
  self._on_request = handler
  return self
end

function MMCStream:on_halt(handler)
  self._on_halt = handler
  return self
end

function MMCStream:halt(err)
  self:reset(err)
  ocall(self._on_halt, self._self, err)
end

function MMCStream:reset(err)
  while true do
    local task = self._queue:pop()
    if not task then break end
    ocall(task.cb, self._self, err)
  end
  self._buffer:reset()
end

end
-------------------------------------------------------------------

-------------------------------------------------------------------
local MMCCommands = ut.class() do

function MMCCommands:__init(stream)
  self._stream = stream
  return self
end

function MMCCommands:_send(data, type, cb)
  return self._stream:request(data, type, cb)
end

-- (key, data, [exptime[, flags[, noreply]]])
function MMCCommands:_set(cmd, ...)
  local cb, key, data, exptime, flags, noreply = cb_args(...)
  return self:_send(
    make_store(cmd, key, data, exptime, flags, noreply),
    REQ_STORE, cb
  )
end

function MMCCommands:set(...)     return self:_set("set", ...)     end

function MMCCommands:add(...)     return self:_set("add", ...)     end

function MMCCommands:replace(...) return self:_set("replace", ...) end

function MMCCommands:append(...)  return self:_set("append", ...)  end

function MMCCommands:prepend(...) return self:_set("prepend", ...) end

-- (key, data, [exptime[, flags[, noreply]]])
function MMCCommands:cas(...)
  local cb, key, data, cas, exptime, flags, noreply = cb_args(...)
  return self:_send(
    make_store("cas", key, data, exptime, flags, noreply, cas),
    REQ_STORE, cb
  )
end

function MMCCommands:get(key, cb)
  return self:_send(
    make_retr("get", key),
    REQ_RETR, cb
  )
end

function MMCCommands:gets(key, cb)
  return self:_send(
    make_retr("gets", key),
    REQ_RETR, cb
  )
end

-- (key[, noreply])
function MMCCommands:delete(...)
  local cb, key, noreply = cb_args(...)
  return self:_send(
    make_change("delete", key, noreply),
    REQ_STORE, cb
  )
end

-- (key[, value[, noreply]])
function MMCCommands:increment(...)
  local cb, key, value, noreply = cb_args(...)
  value = value or 1
  return self:_send(
    make_inc("incr", key, value, noreply),
    REQ_STORE, cb
  )
end

-- (key[, value[, noreply]])
function MMCCommands:decrement(...)
  local cb, key, value, noreply = cb_args(...)
  value = value or 1
  return self:_send(
    make_inc("decr", key, value, noreply),
    REQ_STORE, cb
  )
end

-- (key, value[, noreply])
function MMCCommands:touch(...)
  local cb, key, expire, noreply = cb_args(...)
  assert(expire)
  return self:_send(
    make_inc("touch", key, expire, noreply),
    REQ_STORE, cb
  )
end

function MMCCommands:flush_all(cb)
  return self:_send('flush_all' .. EOL, REQ_STORE, cb)
end

function MMCCommands:version(cb)
  return self:_send('version' .. EOL, REQ_STORE, cb)
end

function MMCCommands:stats(...)
  local cb, key = cb_args(...)
  key = key or ''
  return self:_send('stats ' .. key .. EOL, REQ_STAT, cb)
end

end
-------------------------------------------------------------------

-------------------------------------------------------------------
-- create monitoring timer to be able to reconnect redis connection
-- close this timer before close connection object
local function AutoReconnect(cnn, interval, on_connect, on_disconnect)

  local timer = uv.timer():start(0, interval, function(self)
    self:stop()
    cnn:open()
  end):stop()

  local connected = true

  cnn:on('close', function(self, event, ...)
    local flag = connected

    connected = false

    if flag then on_disconnect(self, ...) end

    if timer:closed() or timer:closing() then
      return
    end

    timer:again()
  end)

  cnn:on('ready', function(self, event, ...)
    connected = true
    on_connect(self, ...)
  end)

  return timer
end
-------------------------------------------------------------------

-------------------------------------------------------------------
local Connection = ut.class() do

local function on_write_handler(cli, err, self)
  if err then
    self._stream:halt(err)
  end
end

local function on_stream_request(self, data, cb)

 if self._max_queue_size then
    local pending = self._stream._queue:size()
    if pending >= self._max_queue_size then
      uv.defer(cb, self,  Error("EQUEUE"))
      self._ee:emit('overflow')
      return false
    end
  end

  if self._ready then
    return self._cnn:write(data, on_write_handler, self)
  end

  if self._cnn then
    self._delay_q:push(data)
    return true
  end

  if cb then uv.defer(cb, self, ENOTCONN) end
end

local function on_stream_halt(self, err)
  if err ~= EOF then
    self._ee:emit('error', err)
  end
  self:_close(err)
end

function Connection:__init(option)
  if type(option) ~= 'table' then
    option = {server = option}
  end
  
  self._host, self._port = split_host(option.server, "127.0.0.1", "11211")
  self._stream           = MMCStream.new(self)
  self._commander        = MMCCommands.new(self._stream)
  self._open_q           = ut.Queue.new()
  self._close_q          = ut.Queue.new()
  self._delay_q          = ut.Queue.new()
  self._ready            = false
  self._ee               = EventEmitter.new{self=self}
  self._max_queue_size   = option.max_queue_size

  if option.reconnect then
    local interval = 30
    if type(option.reconnect) == 'number' then
      interval = option.reconnect * 1000
    end
    self._reconnect_interval = interval
  end

  self._stream
    :on_request(on_stream_request)
    :on_halt(on_stream_halt)

  return self
end

local on_reconnect  = function(self, ...) self._ee:emit('reconnect',  ...) end

local on_disconnect = function(self, ...) self._ee:emit('disconnect', ...) end

function Connection:open(cb)
  if self._ready then
    uv.defer(cb, self)
    return self
  end

  if not self._cnn then
    local ok, err = uv.tcp():connect(self._host, self._port, function(cli, err)
      if err then return self:_close(err) end

      self._ee:emit('open')

      cli:start_read(function(cli, err, data)
        if err then return self._stream:halt(err) end
        self._stream:append(data):execute()
      end)

      self._ready = true
      self._ee:emit('ready')

      while true do
        local data = self._delay_q:pop()
        if not data then break end
        cli:write(data, on_write_handler, self)
      end

      while self._ready do
        local cb = self._open_q:pop()
        if not cb then break end
        cb(self)
      end
    end)

    if not ok then return nil, err end
    self._cnn = ok

    if self._reconnect_interval and not self._reconnect then
      self._reconnect = AutoReconnect(self,
        self._reconnect_interval,
        on_reconnect,
        on_disconnect
      )
    end
 end

  if cb then self._open_q:push(cb) end

  return self
end

function Connection:close(...)
  if self._reconnect then
    self._reconnect:close()
    self._reconnect = nil
  end
  return self:_close(...)
end

function Connection:_close(err, cb)
  if type(err) == 'function' then
    cb, err = err
  end

  if not self._cnn then
    if cb then uv.defer(cb, self) end
    return
  end

  if cb then self._close_q:push(cb) end

  if not (self._cnn:closed() or self._cnn:closing()) then
    self._cnn:close(function()
      self._cnn = nil

      call_q(self._open_q, self, err or EOF)
      self._stream:reset(err or EOF)
      call_q(self._close_q, self, err)
      self._delay_q:reset()

      self._ee:emit('close', err)
    end)
  end

  self._ready = false
end

function Connection:__tostring()
  return string.format("Lua UV Memcached (%s)", tostring(self._cnn))
end

function Connection:connected()
  return not not self._ready
end

function Connection:on(...)
  return self._ee:on(...)
end

function Connection:off(...)
  return self._ee:off(...)
end

function Connection:onAny(...)
  return self._ee:onAny(...)
end

function Connection:offAny(...)
  return self._ee:offAny(...)
end

function Connection:removeAllListeners(...)
  return self._ee:removeAllListeners(...)
end

do -- export commands
  local function cmd(name)
    Connection[name] = function(self, ...)
      return self._commander[name](self._commander, ...)
    end
  end

  cmd"set"
  cmd"add"
  cmd"replace"
  cmd"append"
  cmd"prepend"
  cmd"cas"
  cmd"get"
  cmd"gets"
  cmd"delete"
  cmd"increment"
  cmd"decrement"
  cmd"touch"
  cmd"flush_all"
  cmd"version"
  cmd"stats"
end

end
-------------------------------------------------------------------

return {
  _NAME      = _NAME;
  _VERSION   = _VERSION;
  _COPYRIGHT = _COPYRIGHT;
  _LICENSE   = _LICENSE;

  Connection = Connection;
  self_test  = self_test;
}
