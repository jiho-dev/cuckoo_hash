CC=gcc
CFLAGS=-I. -O2 #-std=c99
CFLAGS+=-W -Wall -Wextra -Wno-unused-function -Wno-unused-parameter
#CFLAGS+=-Werror # treat a warning as an error
#CFLAGS+=-D_REENTRANT -D_GNU_SOURCE
CFLAGS+=-D__DEBUG__ -g

LDFLAGS=-pthread -lpthread

OBJDIR=obj
SRCS = cuckoo.c cuckoo_malloc.c MurmurHash3.c
SRCS += bin_cuckoo.c
SRCS += benchmark.c
DEPS = cuckoo.h 


BIN  = cuckoo_test

#OBJS=$(SRCS:.c=.o)
OBJS=$(patsubst %.c,$(OBJDIR)/%.o,$(SRCS)) 


$(OBJDIR)/%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c $< -o $@

all: tag $(OBJDIR) $(BIN) tag 
	./$(BIN)
#	gdb -ex=r --args ./$(BIN)
#	valgrind  --tool=memcheck --leak-check=yes --show-reachable=yes --log-file="./valgrind.log" ./$(BIN)
#	perf stat -e L1-dcache-load-misses,L1-dcache-loads ./$(BIN)


run: $(BIN)
	./$(BIN)


$(BIN): $(OBJS)
	$(CC) ${LDFLAGS} -o $@ $^ $(CFLAGS)

tag:
	ctags -R

clean:
	rm -rf *.o *.log *~ $(BIN) tags $(OBJDIR)

$(OBJDIR):
	mkdir -p $(OBJDIR)

gcc:
	gcc -c -fPIC -O3 -g3 -pthread \
		-W -Wall -Wextra -Werror -Wno-unused-function \
		-D_REENTRANT -D_GNU_SOURCE \
		-DHTS_INTHASH_USES_MURMUR \
		$(CFILES)

	gcc -shared -fPIC -O3 -Wl,-O1 -Wl,--no-undefined \
		-rdynamic -shared -Wl,-soname=libcoucal.so \
		coucal.o -o libcoucal.so \
		-ldl -lpthread

tests:
	gcc -c -fPIC -O3 -g3 \
		-W -Wall -Wextra -Werror -Wno-unused-function \
		-D_REENTRANT \
		tests.c -o tests.o
#	gcc -fPIC -O3 -Wl,-O1 \
		-lcoucal -L. \
		tests.o -o tests 
	gcc -fPIC -O3 -Wl,-O1 \
		coucal.o tests.o -o tests 

format:
	 uncrustify --no-backup --mtime -c ./formatter.cfg *.[ch]

