# Lab 1 - Reliable Data Transport Protocol

- 517021910872
- 沈嘉欢
- shenjiahuan [at] sjtu.edu.cn

## Design And Implementation

### General Idea

1. This protocol is based on Go-Back-N.
2. One single message may be split into several packets, and each packet will have a sequence ID (32 bits).
3. In data packet, there is a field indicating how large the belonging message is (32 bits), and how large the payload is (8 bits).
4. Checksum is added to the tail of the packet, using CRC32 to calculate checksum of previous 124 bytes.

### Packet format

- Data packet:

  | 4 Bytes     | 4 Bytes      | 1 Byte      | 115 Bytes | 4 Bytes  |
  | ----------- | ------------ | ----------- | --------- | -------- |
  | Sequence ID | Message Size | Packet Size | Payload   | Checksum |

  

- ACK packet

  | 4 Bytes     | 120 Bytes | 4 Bytes  |
  | ----------- | --------- | -------- |
  | Sequence ID | Zero      | Checksum |

### Sender

- Sender will maintain a queue, each element is the payload of one packet.

- Once a request comes from upper layer, sender first split the message into multiple payloads (each will fit into a packet), and push them into the queue.

- Sender will try to pop the queue, make a packet and send it to lower layer if `nextseqid < base + N`, which indicates that the window is not full.

- Once sender receives an ACK, sender's window might move so it will try to send packets.

- There is only one global timer in the protocol.

  - Start condition: Current packet is the first of the window
  - Reset condition: Sender receives one ACK, and there are still packets waiting for ACK
  - Stop condition: Sender receives one ACK, and there is no packets waiting for ACK

  In this design, if timeout is set to `t`, and packet was sent in `t1`, it will not timeout in `t + t1` but will be sometimes later if there comes an ACK of another packet during `t1` and `t + t1`. So timeout may be not accurate in this implementation, but easy to implement, and this design is also shown in textbook "Computer Networking  A Top-Down Approach Featuring the Internet".

- If timeout occurs, all packets with sequence id between `base` to `nextseqid` will be resent. In order to achieve this, packets should be buffered in sender's side.

### Receiver

- Once a request comes from lower layer, receiver first checks its checksum. If failed, the receiver simply drop this request, just as if this packet has been dropped. Otherwise, the receiver checks whether this packet's sequence id is just the expected one, if not, receiver will buffer this request.

- As one message may be split into several packets, receiver may need to:

  1. Save previously received partial packet
  2. Know whether current packet is the last one of the message

  Therefore, receiver maintains a global message object, and copy packet's payload into its appropriate location. Using packet's message size and packet size, receiver can easily understand whether current packet is the last one of the message.

- If current packet is the last one of the message, receiver can send the fully prepared message object to upper layer.

- After finishes processing one packet, the receiver may look into its request buffer to see if packet of next sequence id is already there. If so, use process it. This buffer may greatly increase performance when packet reordering is possible. If receiver simply drop packet not in order, sender will timeout and resend.

### Result

```
$ ./rdt_sim 1000 0.1 100 0.15 0.15 0.15 0                                                                                                                 
## Reliable data transfer simulation with:
	simulation time is 1000.000 seconds
	average message arrival interval is 0.100 seconds
	average message size is 100 bytes
	average out-of-order delivery rate is 15.00%
	average loss rate is 15.00%
	average corrupt rate is 15.00%
	tracing level is 0
Please review these inputs and press <enter> to proceed.

At 0.00s: sender initializing ...
At 0.00s: receiver initializing ...
At 1424.22s: sender finalizing ...
At 1424.22s: receiver finalizing ...

## Simulation completed at time 1424.22s with
	1008120 characters sent
	1008120 characters delivered
	58658 packets passed between the sender and the receiver
## Congratulations! This session is error-free, loss-free, and in order.
```

```
$ ./rdt_sim 1000 0.1 100 0.3 0.3 0.3 0                                                                                                                    
## Reliable data transfer simulation with:
	simulation time is 1000.000 seconds
	average message arrival interval is 0.100 seconds
	average message size is 100 bytes
	average out-of-order delivery rate is 30.00%
	average loss rate is 30.00%
	average corrupt rate is 30.00%
	tracing level is 0
Please review these inputs and press <enter> to proceed.

At 0.00s: sender initializing ...
At 0.00s: receiver initializing ...
At 2460.43s: sender finalizing ...
At 2460.43s: receiver finalizing ...

## Simulation completed at time 2460.43s with
	989597 characters sent
	989597 characters delivered
	65838 packets passed between the sender and the receiver
## Congratulations! This session is error-free, loss-free, and in order.
```

Compared to given reference value, the performance is satisfactory. And I have also tested if no receiver buffer, the performance will decrease greatly.