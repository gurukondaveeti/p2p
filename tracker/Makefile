# tracker/Makefile

CXX      := g++
# -MMD -MP: emit .d files so editing a header rebuilds every .o that includes it
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -pthread -I.. -MMD -MP
LDFLAGS  := -pthread

COMMON_SRCS := ../common/sha1.cpp ../common/net_util.cpp ../common/protocol.cpp
TRACKER_SRCS := oplog.cpp tracker_state.cpp tracker_sync.cpp tracker_server.cpp tracker_main.cpp
SRCS := $(COMMON_SRCS) $(TRACKER_SRCS)
OBJS := $(SRCS:.cpp=.o)
DEPS := $(OBJS:.o=.d)

BIN := tracker

.PHONY: all clean

all: $(BIN)

$(BIN): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(DEPS) $(BIN)

-include $(DEPS)
