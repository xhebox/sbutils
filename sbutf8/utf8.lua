function utf8check(s)
  local r = {}

	local e = string.len(s)
	local c = 1
	local p = 1

	while p <= e do
		local ch1 = string.byte(s, p)
		local ch2, ch3, ch4

		if ch1 < 0x80 then
			-- print(1)
			r[c] = ch1
			p = p + 1
		elseif ch1 < 0xC0 then
			-- print(1.5)
			r[c] = 0x49
			p = p + 1
		elseif ch1 < 0xE0 then
			ch2 = string.byte(s, p+1)
			p = p + 2

			-- print(2, (ch1-0xC0)*0x40 + (ch2-0x80), 0x80, 0x7FF)
			if ch2 == nil or ch2 >= 0xC0 or ch2 < 0x80 or
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

			-- print(3, (ch1-0xE0)*0x1000 + (ch2-0x80)*0x40 + (ch3-0x80), 0x800, 0xFFFF)
			if ch2 == nil or ch2 >= 0xC0 or ch2 < 0x80 or
			ch3 == nil or ch3 >= 0xC0 or ch3 < 0x80 or
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

			-- print(4, (ch1-0xF0)*0x40000 + (ch2-0x80)*0x1000 + (ch3-0x80)*0x40 + (ch4-0x80), 0x10000, 0x10FFFF)
			if ch2 == nil or ch2 >= 0xC0 or ch2 < 0x80 or
			ch3 == nil or ch3 >= 0xC0 or ch3 < 0x80 or
			ch4 == nil or ch4 >= 0xC0 or ch4 < 0x80 or
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
			-- print(5)
			r[c] = 0x49
			p = p + 1
		end

		c = c + 1
	end

	return string.char(unpack(r))
end

print(utf8check("АḂⲤ𝗗𝖤𝗙ꞠꓧȊ𝐉𝜥ꓡ𝑀𝑵Ǭ𝙿𝑄Ŗ𝑆𝒯𝖴𝘝𝘞ꓫŸ𝜡ả𝘢ƀ𝖼ḋếᵮℊ𝙝Ꭵ𝕛кιṃդⱺ𝓅𝘲𝕣𝖘ŧ𝑢ṽẉ𝘅ყž1234567890!@#$%^&*()-_=+[{]};:',<.>/?~Ѧ𝙱ƇᗞΣℱԍҤ١𝔍К𝓛𝓜ƝȎ𝚸𝑄Ṛ𝓢ṮṺƲᏔꓫ𝚈𝚭𝜶Ꮟçძ𝑒𝖿𝗀ḧ𝗂𝐣ҝɭḿ𝕟𝐨𝝔𝕢ṛ𝓼тú𝔳ẃ⤬𝝲𝗓1234567890!@#$%^&*()-_=+[{]};:',<.>/?~𝖠Β𝒞𝘋𝙴𝓕ĢȞỈ𝕵ꓗʟ𝙼ℕ০𝚸𝗤ՀꓢṰǓⅤ𝔚Ⲭ𝑌𝙕𝘢𝕤 "))
