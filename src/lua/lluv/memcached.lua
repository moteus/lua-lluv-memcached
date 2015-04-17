------------------------------------------------------------------
--
--  Author: Alexey Melnichuk <alexeymelnichuck@gmail.com>
--
--  Copyright (C) 2014 Alexey Melnichuk <alexeymelnichuck@gmail.com>
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

local uv = require "lluv"
local ut = require "lluv.utils"
local va = require "vararg"

local EOL = "\r\n"
local WAIT = {}

local REQ_STORE      = 0 -- single line response
local REQ_RETR       = 1 -- line + data response
local REQ_RETR_MULTI = 2 -- line + data response (many keys)

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
})

local function write_with_cb(cli, data, cb)
  cli:write(data, cb)
end

local function cb_args(...)
  local n = select("#", ...)
  local cb = va.range(n, n, ...)
  if type(cb) == 'function' then
    return cb, va.remove(n, ...)
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

local EOF   = uv.error("LIBUV", uv.EOF)

-------------------------------------------------------------------
local MMCStream = ut.class() do

function MMCStream:__init(_self)
  self._buffer = ut.Buffer.new(EOL)
  self._queue  = ut.Queue.new()
  self._self   = _self -- first arg for callbacks

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
    elseif req.type == REQ_RETR then
      local ret = self:_on_retr(req)
      if ret == WAIT then return end
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
      self:halt(err)
    end
  end

  assert(req.len)

  local data = self._buffer:read_n(req.len)
  if not data then return WAIT end
  
  if not req.res then req.res = {} end
  req.res[#req.res + 1] = { data = string.sub(data, 1, -3); flags = req.flags; cas = req.cas; key = req.key; }
  req.len = nil
end

function MMCStream:append(data)
  self._buffer:append(data)
  return self
end

function MMCStream:request(data, type, cb)
  if not self:_on_request(data, cb) then
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
  if self._on_halt then self:_on_halt(err) end
  return
end

function MMCStream:reset(err)
  while true do
    local task = self._queue:pop()
    if not task then break end
    task[CB](self, err)
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
  local cb, key, value, noreply = cb_args(...)
  assert(value)
  return self:_send(
    make_inc("touch", key, value, noreply),
    REQ_STORE, cb
  )
end

end
-------------------------------------------------------------------

-------------------------------------------------------------------
local Connection = ut.class() do

function Connection:__init(server)
  self._host, self._port = split_host(server, "127.0.0.1", "11211")
  self._stream           = MMCStream.new(self)
  self._commander        = MMCCommands.new(self._stream)
  self._open_q           = ut.Queue.new()
  self._close_q          = ut.Queue.new()
  self._delay_q          = ut.Queue.new()
  self._ready            = false

  self._on_message       = nil
  self._on_error         = nil

  local function on_write_error(cli, err)
    if err then self._stream:halt(err) end
  end

  self._on_write_handler = on_write_error

  self._stream
  :on_request(function(s, data, cb)
    if self._ready then
      return self._cnn:write(data, on_write_error)
    end
    if self._cnn then
      self._delay_q:push(data)
      return true
    end
    error('Can not execute command on closed client', 3)
  end)
  :on_halt(function(s, err)
    self:close(err)
    if err ~= EOF then
      ocall(self._on_error, self, err)
    end
  end)

  return self
end

function Connection:open(cb)
  if self._ready then
    uv.defer(cb, self)
    return self
  end

  if not self._cnn then
    local ok, err = uv.tcp():connect(self._host, self._port, function(cli, err)
      if err then return self:close(err) end

      cli:start_read(function(cli, err, data)
        if err then return self._stream:halt(err) end
        self._stream:append(data):execute()
      end)

      while true do
        local data = self._delay_q:pop()
        if not data then break end
        cli:write(data, self._on_write_handler)
      end
      self._ready = true
      while self._ready do
        local cb = self._open_q:pop()
        if not cb then break end
        cb(self)
      end
    end)

    if not ok then return nil, err end
    self._cnn = ok
  end

  if cb then self._open_q:push(cb) end

  return self
end

function Connection:close(err, cb)
  if type(err) == 'function' then
    cb, err = err
  end

  if not self._cnn then
    if cb then uv.defer(cb, self) end
    return
  end

  if cb then self._close_q:push(cb) end

  if not (self._cnn:closed() or self._cnn:closing()) then
    local err = err
    self._cnn:close(function()
      self._cnn = nil

      call_q(self._open_q, self, err or EOF)
      self._stream:reset(err or EOF)
      call_q(self._close_q, self, err)
      self._delay_q:reset()
    end)
  end

  self._ready = false
end

function Connection:on_error(handler)
  self._on_error = handler
  return self
end

function Connection:on_message(handler)
  self._on_message = handler
  return self
end

function Connection:__tostring()
  return string.format("Lua UV Memcached (%s)", tostring(self._cnn))
end

function Connection:connected()
  return not not self._ready
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
end

end
-------------------------------------------------------------------

local function self_test(server, key)
  key = key or "test_key"

  Connection.new(server):open(function(self, err)
    assert(not err, tostring(err))

    self:on_error(function(self, err)
      assert(false, tostring(err))
    end)

    self:delete(key)

    uv.run("once")

    self:delete(key, function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "NOT_FOUND", tostring(ret))
    end)

    uv.run("once")

    self:increment(key, 5, function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "NOT_FOUND", tostring(ret))
    end)

    uv.run("once")

    self:get(key, function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == nil, tostring(ret))
    end)

    uv.run("once")

    self:replace(key, "hello", function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "NOT_STORED", tostring(ret))
    end)

    uv.run("once")

    self:append(key, "hello", function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "NOT_STORED", tostring(ret))
    end)

    uv.run("once")

    self:prepend(key, "hello", function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "NOT_STORED", tostring(ret))
    end)

    uv.run("once")

    self:set(key, "72", 0, 12, function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "STORED", tostring(ret))
    end)

    uv.run("once")

    self:get(key, function(self, err, ret, flags, cas)
      assert(not err, tostring(err))
      assert(ret   == "72", tostring(ret))
      assert(flags == 12,   tostring(flags))
      assert(cas   == nil,  tostring(cas))
    end)

    uv.run("once")

    self:gets(key, function(self, err, ret, flags, cas)
      assert(not err, tostring(err))
      assert(ret   == "72", tostring(ret))
      assert(flags == 12,   tostring(flags))
      assert(type(cas) == "string",  type(cas) .. " - " .. tostring(cas))
    end)

    uv.run("once")

    self:add(key, "hello", function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "NOT_STORED", tostring(ret))
    end)

    uv.run("once")

    self:increment(key, 5, function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "77", tostring(ret))
    end)

    uv.run("once")

    self:decrement(key, 2, function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "75", tostring(ret))
    end)

    uv.run("once")

    self:prepend(key, "1", function(self, err, ret)
      assert(not err, tostring(err))
      assert(ret == "STORED", tostring(ret))
    end)

    uv.run("once")

    self:gets(key, function(self, err, ret, flags, cas)
      assert(not err, tostring(err))
      assert(ret   == "175", tostring(ret))
      assert(flags == 12,   tostring(flags))
      assert(type(cas) == "string",  type(cas) .. " - " .. tostring(cas))

      self:cas(key, "178", cas, function(self, err, ret)
        assert(not err, tostring(err))
        assert(ret == "STORED", tostring(ret))
      end)


      self:cas(key, "177", cas, function(self, err, ret)
        assert(not err, tostring(err))
        assert(ret == "EXISTS", tostring(ret))
      end)

      self:get(key, function()
        print("Done!")
        self:close()
      end)
    end)
  end)

  uv.run(debug.traceback)

end

return {
  Connection = Connection;
  self_test  = self_test;
}
