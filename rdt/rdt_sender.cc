/*
 * FILE: rdt_sender.cc
 * DESCRIPTION: Reliable data transfer sender.
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <queue>

#include "rdt_protocol.h"
#include "rdt_struct.h"
#include "rdt_sender.h"

static seqid_t base;
static seqid_t nextseqid;

static std::vector<packet *> sender_packet_buffer;

struct queue_elem {
    char *data;
    msg_size_t msg_size;
    pkt_size_t pkt_size;
};

static std::queue<queue_elem> packet_queue;

void insert_to_packet_queue(message* msg) {
    int cursor = 0;

    while (msg->size - cursor > maxpayload_size) {
        char* data = new char[maxpayload_size];
        memcpy(data, msg->data + cursor, maxpayload_size);
        packet_queue.push(queue_elem{data, (msg_size_t) msg->size, maxpayload_size});
        cursor += maxpayload_size;
    }

    if (msg->size > cursor) {
        char* data = new char[msg->size - cursor];
        memcpy(data, msg->data + cursor, msg->size - cursor);
        packet_queue.push(queue_elem{data, (msg_size_t) msg->size, (pkt_size_t) (msg->size - cursor)});
    }
}

/* sender initialization, called once at the very beginning */
void Sender_Init() {
    fprintf(stdout, "At %.2fs: sender initializing ...\n", GetSimulationTime());

    base = 0;
    nextseqid = 0;

    sender_packet_buffer = std::vector<packet *>(N);
}

/* sender finalization, called once at the very end.
   you may find that you don't need it, in which case you can leave it blank.
   in certain cases, you might want to take this opportunity to release some 
   memory you allocated in Sender_init(). */
void Sender_Final() {
    fprintf(stdout, "At %.2fs: sender finalizing ...\n", GetSimulationTime());
    for (const auto &pkt : sender_packet_buffer) {
        delete pkt;
    }
}

void send_packets() {
    while (!packet_queue.empty() && nextseqid < base + N) {
        queue_elem elem = packet_queue.front();
        packet_queue.pop();
        auto pkt = pack(nextseqid, elem.data, elem.msg_size, elem.pkt_size);
        if (sender_packet_buffer[nextseqid % N]) {
            delete sender_packet_buffer[nextseqid % N];
        }
        sender_packet_buffer[nextseqid % N] = pkt;
        Sender_ToLowerLayer(pkt);
        if (base == nextseqid) {
            Sender_StartTimer(timeout);
        }

        delete[] elem.data;
        nextseqid++;
    }
}

/* event handler, called when a message is passed from the upper layer at the 
   sender */
void Sender_FromUpperLayer(struct message *msg) {
    insert_to_packet_queue(msg);
    send_packets();
}

/* event handler, called when a packet is passed from the lower layer at the 
   sender */
void Sender_FromLowerLayer(struct packet *pkt) {
    seqid_t seqid = get_seqid(pkt);
    checksum_t checksum = get_checksum(pkt);
    if (checksum != make_checksum(pkt)) {
        return;
    }
    base = seqid + 1;
    if (base == nextseqid) {
        Sender_StopTimer();
    } else {
        Sender_StartTimer(timeout);
    }

    send_packets();
}

/* event handler, called when the timer expires */
void Sender_Timeout() {
    Sender_StartTimer(timeout);
    for (int i = base; i != nextseqid; i++) {
        Sender_ToLowerLayer(sender_packet_buffer[i % N]);
    }
}
