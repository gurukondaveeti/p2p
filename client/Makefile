# client/Makefile

CXX      := g++
# -MMD -MP: emit .d files so editing a header rebuilds every .o that includes it
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -pthread -I.. -MMD -MP
LDFLAGS  := -pthread

COMMON_SRCS := ../common/sha1.cpp ../common/net_util.cpp ../common/protocol.cpp
CLIENT_SRCS := file_registry.cpp tracker_conn.cpp peer_server.cpp downloader.cpp uploader.cpp commands.cpp client_main.cpp
SRCS := $(COMMON_SRCS) $(CLIENT_SRCS)
OBJS := $(SRCS:.cpp=.o)
DEPS := $(OBJS:.o=.d)

BIN := client

.PHONY: all clean

all: $(BIN)

$(BIN): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(DEPS) $(BIN)

-include $(DEPS)
