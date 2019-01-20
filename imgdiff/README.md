# imgdiff

asked by a friend.

this program will read a seriese of images(at least two), and output replace directives to transform the 1st image into the n-th image.

```
./sbanimation 1.png 2.png
#2: ?replace;xxxxxx=xxxxxx....
```

and will output `./new.png` if you can not convert 1.png to other images without modifications. in such case, it will always generate a directive for 1.png.
