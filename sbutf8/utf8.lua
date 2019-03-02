function utf8check(s)
  local r = {}

	local e = string.len(s)
	local c = 0

	for p=0,e,1 do
		local ch1 = string.byte(s, p)
		local ch2, ch3, ch4

		if ch1 < 0x80 then
			r[c] = ch1
		elseif ch1 < 0xC0 then
			r[c] = 'I'
		elseif ch1 < 0xE0 then
			if p+1 >= e then
				r[c] = 'I'
			else
				ch2 = string.byte(s, p+1)
				p = p + 1
			end

			if ch2 > 0xBF or ch2 < 0x80 then
				r[c] = 'I'
			end

			if r[c] ~= 'I' then
				r[c] = (ch1-0xC0)*0x40 + (ch2-0x80)
			end
		elseif ch1 < 0xF0 then
			if p+1 >= e then
				r[c] = 'I'
			else
				ch2 = string.byte(s, p+1)
				p = p + 1
			end

			if ch2 > 0xBF or ch2 < 0x80 then
				r[c] = 'I'
			end

			if p+1 >= e then
				r[c] = 'I'
			else
				ch3 = string.byte(s, p+1)
				p = p + 1
			end

			if ch3 > 0xBF or ch3 < 0x80 then
				r[c] = 'I'
			end

			if r[c] ~= 'I' then
				r[c] = (ch1 - 0xE0)*0x1000 + (ch2-0x80)*0x40 + (ch3-0x80)
			end
		elseif ch1 < 0xF8 then
			if p+1 >= e then
				r[c] = 'I'
			else
				ch2 = string.byte(s, p+1)
				p = p + 1
			end

			if ch2 > 0xBF or ch2 < 0x80 then
				r[c] = 'I'
			end

			if p+1 >= e then
				r[c] = 'I'
			else
				ch3 = string.byte(s, p+1)
				p = p + 1
			end

			if ch3 > 0xBF or ch3 < 0x80 then
				r[c] = 'I'
			end

			if p+1 >= e then
				r[c] = 'I'
			else
				ch4 = string.byte(s, p+1)
				p = p + 1
			end

			if ch4 > 0xBF or ch4 < 0x80 then
				r[c] = 'I'
			end

			if r[c] ~= 'I' then
				r[c] = (ch1-0xF0)*0x40000 + (ch2-0x80)*0x1000 + (ch3-0x80)*0x40 + (ch4-0x80)
			end
		else
			r[c] = 'I'
		end

		c = c + 1
	end

	return string.char(table.unpack(r))
end
