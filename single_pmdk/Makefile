.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -lpmemobj
INCLUDES=-I ./include /home/skian/.local/bin/include
CFLAGS=-O3 -std=c++11 -g

output = btree 

all: main

main: src/test.cpp
	g++ $(CFLAGS) -o btree src/test.cpp $(LIBS)

clean: 
	rm $(output)
