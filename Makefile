all: lmdbsh

lmdbsh: lmdbsh.o
	g++ -o lmdbsh *.o /usr/local/lib/liblmdb.a -pthread

lmdbsh.o: lmdbsh.cpp
	g++ -c lmdbsh.cpp

install:
	mkdir -p /usr/local/bin
	cp lmdbsh /usr/local/bin/lmdbsh

clean:
	rm -f *.o
	rm -f lmdbsh


