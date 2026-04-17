CC      = gcc
CFLAGS  = -Wall -O2
LDFLAGS = -lmosquitto -lpthread

all: pub sub

pub: pub.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) -lsqlite3

sub: sub.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) -lws

clean:
	rm -f pub sub

.PHONY: all clean