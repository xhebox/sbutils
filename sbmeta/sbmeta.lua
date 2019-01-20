local sbmeta = {
	_VERSION = 'sbmeta.lua 0.1',
	_URL     = 'http://github.com/xhebox/sbmeta',
	_AUTHOR = 'xw897002528@gmail.com',
	_DESCRIPTION = 'add starbound metatable for table manually',
	_LICENSE = [[
MIT License

Copyright (c) 2019 xhe

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
]]
}

function sbmeta:record(path, value)
	if type(value) == "table" then
		local t = value["1"]
		if t then
			local elem = {}
			for _, v in pairs(path) do
				table.insert(elem, v)
			end
			table.insert(self.array_path, elem)
			value["xhedalaotqlwsl1"] = t
			value["1"] = nil
		end

		for k, v in pairs(value) do 
			table.insert(path, k)
			self:record(path, v)
			table.remove(path)
		end
	end
end

function sbmeta:recover(value)
	local t = self.array_path
	for i = #t, 1, -1 do
		local val = value
		for _, k in pairs(t[i]) do
			val = val[k]
		end
		val["1"] = val["xhedalaotqlwsl1"]
		val["xhedalaotqlwsl1"] = nil
	end
end

function sbmeta:addMetatable(value)
	self.array_path = {}

	-- 1. modify index of {["1"]=xxx} like table
	-- avoid mergeJson treat it like an array
	self:record({}, value)

	-- 2. add metatable
	value = sb.jsonMerge({}, value)

	-- 3. recover array index
	self:recover(value)

	return value
end

setmetatable(sbmeta, { __call = function(value) return sbmeta:addMetatable(value) end })

return sbmeta
