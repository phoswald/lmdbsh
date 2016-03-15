all: lmdbsh

lmdbsh: lmdbsh.o lmdb-wrappers.o
	g++ -o lmdbsh *.o /usr/local/lib/liblmdb.a -pthread

lmdbsh.o: lmdbsh.cpp lmdb-wrappers.h
	g++ -c lmdbsh.cpp

lmdb-wrappers.o: lmdb-wrappers.cpp lmdb-wrappers.h
	g++ -c lmdb-wrappers.cpp

install:
	mkdir -p /usr/local/bin
	cp lmdbsh /usr/local/bin/lmdbsh

clean:
	rm -f *.o
	rm -f lmdbsh

