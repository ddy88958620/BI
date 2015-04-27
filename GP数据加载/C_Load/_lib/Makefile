INCDIR=../_include
GPINCDIR=/usr/local/greenplum-db/include
PUBSRC=$(INCDIR)/comm.h
ECPG=ecpg
CC=cc
CFLAG=-c -I$(INCDIR) -t
OFLAG=-I$(GPINCDIR) -I$(INCDIR) -c
all:commfunc.o

commfunc.o:commfunc.pgc $(PUBSRC)
	$(ECPG) $(CFLAG) commfunc.pgc
	$(CC) $(OFLAG) commfunc.c
