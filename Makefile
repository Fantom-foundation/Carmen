# --------------------------------------------------------------------------
# Makefile for Carmen
#
# v1.0 (2023/09/05) - Initial version
#
# (c) Fantom Foundation, 2023
# --------------------------------------------------------------------------

.PHONY: all clean

all: carmen-cpp

# this target builds the C++ library required by Go
carmen-cpp: 
	@cd ./go/lib ; \
	./build_libcarmen.sh ;

clean:
	cd ./go ; \
	rm -f lib/libcarmen.so ; \
	cd ../cpp ; \
	bazel clean ; \