# Top-level Makefile -- convenience wrapper around tracker/ and client/.
#
#   make            build both binaries
#   make clean      remove both binaries and all .o files
#   make test       build, then run the automated end-to-end smoke test

.PHONY: all clean test tracker client

all: tracker client

tracker:
	$(MAKE) -C tracker

client:
	$(MAKE) -C client

clean:
	$(MAKE) -C tracker clean
	$(MAKE) -C client clean

test: all
	bash test/smoke_test.sh
