
SRC = shed.c
CFLAGS = -Wall -Wextra -g -Wno-missing-field-initializers -Wno-unused-parameter
LDFLAGS = -pthread
LOADLIBES = -lssh
EXEC = shed

all: $(EXEC)
