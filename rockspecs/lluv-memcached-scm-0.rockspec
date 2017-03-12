package = "lluv-memcached"
version = "scm-0"

source = {
  url = "https://github.com/moteus/lua-lluv-memcached/archive/master.zip",
  dir = "lua-lluv-memcached-master",
}

description = {
  summary    = "Memcached client for lua-lluv library",
  homepage   = "https://github.com/moteus/lua-lluv-memcached",
  license    = "MIT/X11",
  maintainer = "Alexey Melnichuk",
  detailed   = [[
  ]],
}

dependencies = {
  "lua >= 5.1, < 5.4",
  "lluv > 0.1.1",
  "eventemitter",
  -- "vararg",
}

build = {
  copy_directories = {'test'},

  type = "builtin",

  modules = {
    ["lluv.memcached"] = "src/lua/lluv/memcached.lua",
  }
}
