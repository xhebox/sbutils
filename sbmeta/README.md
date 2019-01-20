# sbmeta

many starbound mods are able to generate items on fly based on a JSON string, but has some limitations. items are not able to be spawned correctly, though JSON has been converted to lua table perfectly. the reason behind is that starbound needs table with a metatable, which contains some more info and even a callback function to work.

and sb.jsonMerge is able to add needed metatable for pure lua table, but it will convert `{["1"]=2, ["2"]=3}`, e.g. a number-string-indexed table into an array. this lua script provide a solution for that by renaming 1 before jsonMerge and recover it later.

```lua
sbmeta = require "sbmeta"
table = sbmeta(table)
```

also, i include a modified json.lua(rxi/json.lua) that will add metatable natively.

**starbound has no support for traditional require usage, since return is not executed for safe.** you should delete `local sbmeta & return sbmeta` in sbmeta.lua, same to json.lua. then use it like so:

```lua
require "/sbmeta.lua"
require "/json.lua"

json.decode(str)
sbmeta(table)
```

must require scripts in absolute path, refer to `/scripts/*.lua` for more details.
