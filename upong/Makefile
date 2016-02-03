CPPFLAGS=-std=c++11
THRIFT_GEN+= -Igen-cpp
CPP=g++
LDFLAGS=-g $(THRIFT_GEN)
LDLIBS=-lfolly -lgflags -lpthread -lglog
THRIFT=thrift

all: upong

upong: NoradTargetThread.o
	$(CPP) $(LDFLAGS) $(CPPFLAGS) NoradTargetThread.o Main.cpp -o upong $(LDLIBS)

NoradTargetThread.o:
	$(CPP) $(LDFLAGS) $(CPPFLAGS) -c  NoradTargetThread.cpp

clean:
	rm -f *.o upong
