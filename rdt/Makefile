# NOTE: Feel free to change the makefile to suit your own need.

# compile and link flags
CCFLAGS = -Wall -g -std=c++11
LDFLAGS = -Wall -g -std=c++11

# make rules
TARGETS = rdt_sim 

all: $(TARGETS)

.cc.o:
	g++ $(CCFLAGS) -c -o $@ $<

crc.o: crc.h

rdt_protocol.o: rdt_protocol.h

rdt_sender.o: 	rdt_struct.h rdt_sender.h

rdt_receiver.o:	rdt_struct.h rdt_receiver.h 

rdt_sim.o: 	rdt_struct.h

rdt_sim: rdt_sim.o rdt_sender.o rdt_receiver.o rdt_protocol.o crc.o
	g++ $(LDFLAGS) -o $@ $^

clean:
	rm -f *~ *.o $(TARGETS)
