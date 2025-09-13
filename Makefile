PROJECT_ROOT = $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

OBJS = client.o

ifeq ($(BUILD_MODE),debug)
	CFLAGS += -g -O0
else ifeq ($(BUILD_MODE),run)
	CFLAGS += -O2
else ifeq ($(BUILD_MODE),profile)
	CFLAGS += -g -pg -fprofile-arcs -ftest-coverage
	LDFLAGS += -pg -fprofile-arcs -ftest-coverage
	EXTRA_CLEAN += client.gcda client.gcno $(PROJECT_ROOT)gmon.out
	EXTRA_CMDS = rm -rf client.gcda
else
    $(error Build mode $(BUILD_MODE) not supported by this Makefile)
endif

	CFLAGS += -pthread
	LDFLAGS += -pthread
	
all:	client

client:	$(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^
	$(EXTRA_CMDS)

%.o:	$(PROJECT_ROOT)%.cpp
	$(CXX) -c $(CFLAGS) $(CXXFLAGS) $(CPPFLAGS) -o $@ $<

%.o:	$(PROJECT_ROOT)%.c
	$(CC) -c $(CFLAGS) $(CPPFLAGS) -o $@ $<

clean:
	rm -fr client $(OBJS) $(EXTRA_CLEAN)
