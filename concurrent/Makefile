.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -lpthread
INCLUDES=-I./include
CFLAGS=-O0 -std=c++11 -g 

output = btree_concurrent btree_concurrent_mixed

all: main

main: src/test.cpp
	g++ $(CFLAGS) -o btree_concurrent src/test.cpp $(LIBS) -DCONCURRENT
	g++ $(CFLAGS) -o btree_concurrent_mixed src/test.cpp $(LIBS) -DCONCURRENT -DMIXED

clean: 
	rm $(output)
