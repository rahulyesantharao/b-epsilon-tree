CXX := g++
CXXFLAGS := -std=c++11 
LDFLAGS := 
BUILD := ./build
OBJ_DIR := $(BUILD)/obj
APP_DIR := $(BUILD)/app
RUNTIME_DIR := $(APP_DIR)/tree
TARGET := test
INCLUDE := -Iinc/
SRC			:= \
				$(wildcard src/block_manager/*.cpp) \
				$(wildcard src/lru_cache/*.cpp) \
				$(wildcard src/be_tree/*.cpp) \
				$(wildcard src/*.cpp) \

#SRC := $(wildcard src/*.cpp)
OBJECTS := $(SRC:%.cpp=$(OBJ_DIR)/%.o)

ifeq ($(DEBUG),1)
	CXXFLAGS += -O0 -g -DDEBUG 
else
	CXXFLAGS += -O3 -DNDEBUG
endif

all: build $(APP_DIR)/$(TARGET)

$(OBJ_DIR)/%.o: %.cpp
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(INCLUDE) -o $@ -c $<

$(APP_DIR)/$(TARGET): $(OBJECTS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(INCLUDE) $(LDFLAGS) -o $(APP_DIR)/$(TARGET) $(OBJECTS)

.PHONY: all build clean

build:
	@mkdir -p $(APP_DIR)
	@mkdir -p $(OBJ_DIR)
	@mkdir -p $(RUNTIME_DIR)

clean:
	-@rm -rvf $(RUNTIME_DIR)/*
	-@rm -rvf $(OBJ_DIR)/*
	-@rm -rvf $(APP_DIR)/*
