# dumpsbvj01

```
Usage of ./dumpsbvj01:
  -i string
        versioned json file (default "input")
  -m string
        vjmagic/vj/raw (default "vj")
  -n int
        skip first n bytes
  -o string
        output json (default "stdout")
```

this program will read a versioned json, unserialize it.

three modes there:

+ vjmagic: a versioned json with header/magic.
+ vj: a versioned json with header, but without magic.
+ raw: a versioned json without header/magic.

you can skip first n bytes by '-n' flag.
