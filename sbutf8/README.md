# sbmeta

starbound checks invalid utf8 sequences, and actually does not support utf8 bigger than 4 bytes, so this will trigger exception when passing it to starbound built-in functions. this function replace the invalid with 'I'. 
