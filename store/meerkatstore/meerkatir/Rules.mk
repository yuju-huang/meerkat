d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc)

OBJS-meerkatstore-client := $(OBJS-meerkatir-client)             \
        $(LIB-store-frontend)                           \
        $(LIB-store-common)                             \
        $(LIB-ziplog)                                   \
		$(o)client.o
