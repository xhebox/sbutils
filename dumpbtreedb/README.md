# dumpbtreedb

```
Usage of ./dumpbtreedb:
  -i string
        input file (default "input")
```

this program will read a btreedb5 file, extract it into the current directory.

btreedb5 has two b+ btree, and the tree containing more records is the main tree, the other is the snapshot, e.g. the bakup(i guess).

it results a lot of files started with 'tree1_' or 'tree2_'. every file is a record and the filename is the key in hex.

world metadata is a versioned json with two int32 saying world size before all the things. you can extract it with `./dumpsbvj01 -i firstrecord -n 8`
