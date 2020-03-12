//
// Created by 沈嘉欢 on 2020/3/12.
//

#ifndef RDT_SIM_RDT_PROTOCOL_H
#define RDT_SIM_RDT_PROTOCOL_H

#include <cstdint>
#include "rdt_struct.h"

using seqid_t = uint32_t;
using msg_size_t = uint32_t;
using pkt_size_t = uint8_t;
using checksum_t = uint32_t;

const seqid_t N = 10;
const double timeout = 0.3;

const pkt_size_t header_size = sizeof(seqid_t) + sizeof(msg_size_t) + sizeof(pkt_size_t);
const pkt_size_t footer_size = sizeof(checksum_t);
const pkt_size_t maxpayload_size = RDT_PKTSIZE - header_size - footer_size;

checksum_t get_checksum(packet *pkt);
seqid_t get_seqid(packet *pkt);
packet *pack(seqid_t seqid, const char *data, msg_size_t msg_size, pkt_size_t pkt_size);
checksum_t make_checksum(packet *pkt);
msg_size_t get_msg_size(packet *pkt);
pkt_size_t get_pkt_size(packet *pkt);
packet* make_ack(seqid_t seqid);
char* get_data(packet *pkt);

#endif //RDT_SIM_RDT_PROTOCOL_H
