MODULE_big = dmv
OBJS = dmv.o wal_reader.o
DATA = dmv--0.0.1.sql
EXTENSION = dmv

override CPPFLAGS := -std=gnu99 $(CPPFLAGS)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
