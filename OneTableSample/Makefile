include $(PCF_DEV_HOME)/makehead

ONE_TABLE_DIR = $(PCF_DEV_HOME)/SRC/ONE_TABLE
ONE_TABLE_LIB = -lOneTable

EXE 	= OT
SRCS 	= $(wildcard *.cpp)
HEADERS = $(filter-out, $(wildcard *.hpp))
OBJS 	= $(SRCS:.cpp=.o)

CFLAGS += -I$(ONE_TABLE_DIR)
CFLAGS += -D__OT_DEBUG

LDFLAGS += -L$(ONE_TABLE_DIR) $(ONE_TABLE_LIB) 
#$(IDFLAGS) += -L$(HAZELCAST_LIB_PATH) -l$(HAZELCAST_LIB)

all :: $(EXE)
$(EXE) : $(OBJS) $(HEADERS)
	$(CXX) -o $(EXE) $(OBJS) $(LDFLAGS)

clean:
	$(RM) $(OBJS) $(EXE) core.* *.d
