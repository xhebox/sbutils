function utf8check(s)
  local r = {}

	local e = string.len(s)
	local c = 1
	local p = 1

	while p <= e do
		local ch1 = string.byte(s, p)
		local ch2, ch3, ch4

		if ch1 < 0x80 then
			r[c] = ch1
			p = p + 1
		elseif ch1 < 0xC0 then
			r[c] = 0x49
			p = p + 1
		elseif ch1 < 0xE0 then
			ch2 = string.byte(s, p+1)
			p = p + 2

			if ch2 == nil or ch2 > 0xBF or ch2 < 0x80 or
			((ch1-0xC0) * 0x4 + (ch2-0x80) / 0xF) < 0x8 then
				r[c] = 0x49
			end

			if r[c] ~= 0x49 then
				r[c] = ch1
				r[c+1] = ch2
				c = c + 1
			end
		elseif ch1 < 0xF0 then
			ch2 = string.byte(s, p+1)
			ch3 = string.byte(s, p+2)
			p = p + 3

			if ch2 == nil or ch2 > 0xBF or ch2 < 0x80 or
			ch3 == nil or ch3 > 0xBF or ch3 < 0x80 or
			((ch1-0xE0) * 0xF + (ch2-0x80) / 0x4) < 0x8 then
				r[c] = 0x49
			end

			if r[c] ~= 0x49 then
				r[c] = ch1
				r[c+1] = ch2
				r[c+2] = ch3
				c = c + 2
			end
		elseif ch1 < 0xF8 then
			ch2 = string.byte(s, p+1)
			ch3 = string.byte(s, p+2)
			ch4 = string.byte(s, p+3)
			p = p + 4

			if ch2 == nil or ch2 > 0xBF or ch2 < 0x80 or
			ch3 == nil or ch3 > 0xBF or ch3 < 0x80 or
			ch4 == nil or ch4 > 0xBF or ch4 < 0x80 or
			((ch2-0x80) / 0xF) < 0x1 then
				r[c] = 0x49
			end

			if r[c] ~= 0x49 then
				r[c] = ch1
				r[c+1] = ch2
				r[c+2] = ch3
				r[c+3] = ch4
				c = c + 3
			end
		else
			r[c] = 0x49
			p = p + 1
		end

		c = c + 1
	end

	return string.char(unpack(r))
end
