INCDIR=./_include
GPINCDIR=/usr/local/greenplum-db/include
PUBSRC=$(INCDIR)/comm.h
ECPG=ecpg
CC=cc
COMM=./_lib/commfunc.o
GPLIBDIR=/usr/local/greenplum-db/lib
CFLAG=-c -I$(INCDIR) -t
OFLAG=-I$(GPINCDIR) -I$(INCDIR) -c
XFLAG=-L/usr/local/greenplum-db/lib -lecpg -o 

all:syndata
clear:
	rm syndata

syndata:syndata.pgc $(PUBSRC)
	$(ECPG) $(CFLAG) syndata.pgc
	$(CC) $(OFLAG) syndata.c
	$(CC) $(XFLAG) syndata syndata.o $(COMM)
