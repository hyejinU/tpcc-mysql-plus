# MySQL 8.4.5 build 경로를 직접 지정
MYSQL_HOME = /root/snu/mysql-8.4.5-build
INCDIR = $(MYSQL_HOME)/include
LIBDIR = $(MYSQL_HOME)/lib

CFLAGS = -w -O3 -g -I. -I$(INCDIR)
LDFLAGS = -L$(LIBDIR) -lmysqlclient -lpthread -lz -lm -ldl -lrt

TRANSACTIONS = neword.o payment.o ordstat.o delivery.o slev.o credit.o
OBJS = main.o spt_proc.o driver.o support.o sequence.o rthist.o sb_percentile.o $(TRANSACTIONS)

.SUFFIXES:
.SUFFIXES: .o .c

.c.o:
	$(CC) $(CFLAGS) -c $*.c

all: ../tpcc_load ../tpcc_start

../tpcc_load: load.o support.o
	$(CC) load.o support.o $(LDFLAGS) -o ../tpcc_load

../tpcc_start: $(OBJS)
	$(CC) $(OBJS) $(LDFLAGS) -o ../tpcc_start

clean:
	rm -f *.o ../tpcc_load ../tpcc_start
