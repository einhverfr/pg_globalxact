EXTENSION = pg_globalxact
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
        sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
PG_CONFIG ?= pg_config
DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
MODULE_big = $(EXTENSION)
OBJS 	= $(patsubst %.c,%.o,$(wildcard src/*.c))
REGRESS_PREP = tmptest
TESTS    = $(wildcard test/sql/*.sql)
REGRESS   = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-language=plpgsql --load-extension=$(EXTENSION)
PG_CPPFLAGS = --std=c99 -Wall -Wextra -Wno-unused-parameter -Iinclude -I$(libpq_srcdir)
PG_CFLAGS = -Wno-implicit-fallthrough
SHLIB_LINK 	 = $(libpq)
include $(PGXS)
$(EXTENSION)--$(EXTVERSION).sql: $(EXTENSION).sql
	cp $< $@
all: $(EXTENSION)--$(EXTVERSION).sql $(EXTENSION).so
tmptest:
	cp -a test/* /tmp


