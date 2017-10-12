## Install
* download libmongoc from [http://mongoc.org/]
* install libmongoc to /usr/local/ by 

```
./configure --disable-automatic-init-and-cleanup --prefix=/usr/local/
make -j5
make install
```
* install by go get

```
go get gopkg.in/mongoc.v1
```
