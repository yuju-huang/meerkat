# Build ziplog

d := $(dir $(lastword $(MAKEFILE_LIST)))

ziplog_src := $(addprefix $(d), ziplog/src/)
ziplog_network := $(addprefix $(ziplog_src), network)
ziplog_util := $(addprefix $(ziplog_src), util)
ziplog_client := $(addprefix $(ziplog_src), client)
SRCS += $(ziplog_network)/buffer.cc $(ziplog_network)/manager.cc $(ziplog_network)/send_queue.cc $(ziplog_network)/recv_queue.cc \
        $(ziplog_util)/util.cc $(ziplog_client)/client.cc

ziplog_obj := $(addprefix $(o), ziplog/src/)
ziplog_network_obj := $(addprefix $(ziplog_obj), network)
ziplog_util_obj := $(addprefix $(ziplog_obj), util)
ziplog_client_obj := $(addprefix $(ziplog_obj), client)
LIB-ziplog := $(ziplog_network_obj)/buffer.o $(ziplog_network_obj)/manager.o $(ziplog_network_obj)/send_queue.o $(ziplog_network_obj)/recv_queue.o \
             $(ziplog_util_obj)/util.o $(ziplog_client_obj)/client.o
