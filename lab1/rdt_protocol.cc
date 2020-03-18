#include <cstring>
#include <cstdlib>
#include "rdt_protocol.h"
#include "crc.h"

checksum_t get_checksum(packet *pkt) {
    checksum_t val;
    memcpy(&val, pkt->data + header_size + maxpayload_size, sizeof(checksum_t));
    return val;
}

seqid_t get_seqid(packet *pkt) {
    seqid_t val;
    memcpy(&val, pkt->data, sizeof(seqid_t));
    return val;
}

checksum_t make_checksum(packet *pkt) {
    return (checksum_t) xcrc32((const unsigned char *) (pkt->data), (int) (header_size + maxpayload_size), 0xffffffff);
}

packet *pack(seqid_t seqid, const char *data, msg_size_t msg_size, pkt_size_t pkt_size) {
    auto pkt = (packet *) malloc(sizeof(packet));
    memcpy(pkt->data, &seqid, sizeof(seqid_t));
    memcpy(pkt->data + sizeof(seqid_t), &msg_size, sizeof(msg_size_t));
    memcpy(pkt->data + sizeof(seqid_t) + sizeof(msg_size_t), &pkt_size, sizeof(pkt_size));
    memcpy(pkt->data + header_size, data, pkt_size);
    checksum_t checksum = make_checksum(pkt);
    memcpy(pkt->data + header_size + maxpayload_size, &checksum, sizeof(checksum_t));
    return pkt;
}

msg_size_t get_msg_size(packet *pkt) {
    msg_size_t val;
    memcpy(&val, pkt->data + sizeof(seqid_t), sizeof(msg_size_t));
    return val;
}

pkt_size_t get_pkt_size(packet *pkt) {
    pkt_size_t val;
    memcpy(&val, pkt->data + sizeof(seqid_t) + sizeof(msg_size_t), sizeof(pkt_size_t));
    return val;
}

packet* make_ack(seqid_t seqid) {
    packet* pkt = (packet *)malloc(sizeof(packet));
    memcpy(pkt->data, &seqid, sizeof(seqid_t));
    checksum_t checksum = make_checksum(pkt);
    memcpy(pkt->data + header_size + maxpayload_size, &checksum, sizeof(checksum_t));
    return pkt;
}

char* get_data(packet *pkt) {
    return pkt->data + header_size;
}