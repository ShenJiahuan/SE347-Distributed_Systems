/*
 * FILE: rdt_receiver.cc
 * DESCRIPTION: Reliable data transfer receiver.
 */


#include <cstdio>
#include <cstring>
#include <map>

#include "rdt_protocol.h"
#include "rdt_struct.h"
#include "rdt_receiver.h"

using seqid_t = uint32_t;
using msg_size_t = uint32_t;
using pkt_size_t = uint8_t;
using checksum_t = uint32_t;

static seqid_t expectedseqid;

static message *msg_buffer;
static msg_size_t buffer_size;

static packet *last_ack;

static std::map<seqid_t, packet *> receiver_packet_buffer;


/* receiver initialization, called once at the very beginning */
void Receiver_Init() {
    fprintf(stdout, "At %.2fs: receiver initializing ...\n", GetSimulationTime());
    expectedseqid = 0;
    msg_buffer = new message();
    memset(msg_buffer, 0, sizeof(message));
    buffer_size = 0;
}

/* receiver finalization, called once at the very end.
   you may find that you don't need it, in which case you can leave it blank.
   in certain cases, you might want to use this opportunity to release some 
   memory you allocated in Receiver_init(). */
void Receiver_Final() {
    fprintf(stdout, "At %.2fs: receiver finalizing ...\n", GetSimulationTime());
}

/* event handler, called when a packet is passed from the lower layer at the 
   receiver */
void Receiver_FromLowerLayer(struct packet *pkt) {

    seqid_t seqid = get_seqid(pkt);
    msg_size_t msg_size = get_msg_size(pkt);
    pkt_size_t pkt_size = get_pkt_size(pkt);
    checksum_t checksum = get_checksum(pkt);

    if (checksum != make_checksum(pkt)) {
        return;
    }

    if (seqid != expectedseqid) {
        if (last_ack) {
            Receiver_ToLowerLayer(last_ack);
        }
        if (seqid > expectedseqid) {
            auto pkt1 = new packet();
            memcpy(pkt1, pkt, sizeof(packet));
            receiver_packet_buffer.insert(std::make_pair(seqid, pkt1));
        }
        return;
    }
    expectedseqid++;

    if (buffer_size == 0) {
        msg_buffer->size = msg_size;
        msg_buffer->data = new char[msg_size];
    }

    memcpy(msg_buffer->data + buffer_size, get_data(pkt), pkt_size);
    buffer_size += pkt_size;
    if (buffer_size == (msg_size_t) msg_buffer->size) {
        Receiver_ToUpperLayer(msg_buffer);

        delete[] msg_buffer->data;
        msg_buffer->size = 0;
        msg_buffer->data = nullptr;
        buffer_size = 0;
    }

    delete last_ack;
    last_ack = make_ack(seqid);
    Receiver_ToLowerLayer(last_ack);

    if (receiver_packet_buffer.find(expectedseqid) != receiver_packet_buffer.end()) {
        auto buffered_id = expectedseqid;
        auto buffered_pkt = receiver_packet_buffer[buffered_id];
        Receiver_FromLowerLayer(buffered_pkt);
        receiver_packet_buffer.erase(buffered_id);
        delete buffered_pkt;
    }
}
