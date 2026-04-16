CC      = gcc
CFLAGS  = -Wall -O2
LDFLAGS = -lmosquitto -lpthread

all: publisher subscriber_ws

publisher: publisher.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) -lsqlite3

subscriber_ws: subscriber_ws.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) -lws

clean:
	rm -f publisher subscriber_ws

.PHONY: all clean
