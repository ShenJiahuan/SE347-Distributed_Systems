#include <stdint.h>
#include <inttypes.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_ether.h>
#include <rte_ip.h>

#define RX_RING_SIZE 1024
#define TX_RING_SIZE 1024

#define NUM_MBUFS 8191
#define MBUF_CACHE_SIZE 250
#define BURST_SIZE 32

static const struct rte_eth_conf port_conf_default = {
	.rxmode = {
		.max_rx_pkt_len = RTE_ETHER_MAX_LEN,
	},
};

const struct rte_ether_addr dst_mac = {{0x00, 0x1c, 0x42, 0x00, 0x00, 0x09}};

const uint32_t src_ip = RTE_IPV4(10, 37, 129, 4);
const uint32_t dst_ip = RTE_IPV4(10, 37, 129, 2);
const uint16_t src_port = 1926;
const uint16_t dst_port = 2020;

const char* payloads[] = {
	"Too young too simple!",
	"Sometimes naive!",
	"I'm angry!",
	"Exciting!",
	"+1s"
};

// In DPDK, a "port" is a NIC. We will use the first NIC DPDK finds.
static inline int
port_init(uint16_t port, struct rte_mempool *mbuf_pool)
{
	struct rte_eth_conf port_conf = port_conf_default;
	const uint16_t rx_rings = 1, tx_rings = 1;
	uint16_t nb_rxd = RX_RING_SIZE;
	uint16_t nb_txd = TX_RING_SIZE;
	int retval;
	uint16_t q;
	struct rte_eth_dev_info dev_info;
	struct rte_eth_txconf txconf;

	if (!rte_eth_dev_is_valid_port(port))
		return -1;

	retval = rte_eth_dev_info_get(port, &dev_info);
	if (retval != 0) {
		printf("Error during getting device (port %u) info: %s\n",
				port, strerror(-retval));
		return retval;
	}

	if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE)
		port_conf.txmode.offloads |=
			DEV_TX_OFFLOAD_MBUF_FAST_FREE;

	/* Configure the Ethernet device. */
	retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
	if (retval != 0)
		return retval;

	retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
	if (retval != 0)
		return retval;

	/* Allocate and set up 1 RX queue per Ethernet port. */
	for (q = 0; q < rx_rings; q++) {
		retval = rte_eth_rx_queue_setup(port, q, nb_rxd,
				rte_eth_dev_socket_id(port), NULL, mbuf_pool);
		if (retval < 0)
			return retval;
	}

	txconf = dev_info.default_txconf;
	txconf.offloads = port_conf.txmode.offloads;
	/* Allocate and set up 1 TX queue per Ethernet port. */
	for (q = 0; q < tx_rings; q++) {
		retval = rte_eth_tx_queue_setup(port, q, nb_txd,
				rte_eth_dev_socket_id(port), &txconf);
		if (retval < 0)
			return retval;
	}

	/* Start the Ethernet port. */
	retval = rte_eth_dev_start(port);
	if (retval < 0)
		return retval;

	/* Display the port MAC address. */
	struct rte_ether_addr addr;
	retval = rte_eth_macaddr_get(port, &addr);
	if (retval != 0)
		return retval;

	printf("Port %u MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
			   " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
			port,
			addr.addr_bytes[0], addr.addr_bytes[1],
			addr.addr_bytes[2], addr.addr_bytes[3],
			addr.addr_bytes[4], addr.addr_bytes[5]);

	/* Enable RX in promiscuous mode for the Ethernet device. */
	retval = rte_eth_promiscuous_enable(port);
	if (retval != 0)
		return retval;

	return 0;
}

/*
 * The lcore main. This is the main thread that does the work, reading from
 * an input port and writing to an output port.
 */
static __attribute__((noreturn)) void
lcore_main(uint16_t port, struct rte_mempool *mbuf_pool)
{
	const int total_length = 64;
	int ip_length = total_length - sizeof(struct rte_ether_hdr);
	int udp_length = ip_length - sizeof(struct rte_ipv4_hdr);

	/* Run until the application is quit or killed. */
	for (;;) {
		struct rte_mbuf *buf[BURST_SIZE];
		struct rte_ether_addr s_addr;
		rte_eth_macaddr_get(port, &s_addr);
		struct rte_ether_addr d_addr = dst_mac;
		
		for (int i = 0; i < BURST_SIZE; i++) {
			buf[i] = rte_pktmbuf_alloc(mbuf_pool);
			buf[i]->pkt_len = total_length;
			buf[i]->data_len = total_length;

			struct rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(buf[i], struct rte_ether_hdr *);
			eth_hdr->s_addr = s_addr;
			eth_hdr->d_addr = d_addr;
			eth_hdr->ether_type = rte_cpu_to_be_16(0x0800);

			struct rte_ipv4_hdr *ip_hdr = (struct rte_ipv4_hdr *) (eth_hdr + 1);
			ip_hdr->version_ihl = 0x45;
			ip_hdr->type_of_service = 0;
			ip_hdr->total_length = rte_cpu_to_be_16(ip_length);
			ip_hdr->packet_id = 0;
			ip_hdr->fragment_offset = 0;
			ip_hdr->time_to_live = 64;
			ip_hdr->next_proto_id = 0x11;
			ip_hdr->src_addr = rte_cpu_to_be_32(src_ip);
			ip_hdr->dst_addr = rte_cpu_to_be_32(dst_ip);
			ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

			struct rte_udp_hdr *udp_hdr = (struct rte_udp_hdr *) (ip_hdr + 1);
			udp_hdr->src_port = rte_cpu_to_be_16(src_port);
			udp_hdr->dst_port = rte_cpu_to_be_16(dst_port);
			udp_hdr->dgram_len = rte_cpu_to_be_16(udp_length);
			udp_hdr->dgram_cksum = 0;

			int choice = rand() % (sizeof(payloads) / sizeof(payloads[0]));
			const char *payload = payloads[choice];
			memset((char *) (udp_hdr + 1), 0, udp_length);
			strcpy((char *) (udp_hdr + 1), payload);

		}
		
		rte_eth_tx_burst(port, 0, buf, BURST_SIZE);

		for (int i = 0; i < BURST_SIZE; i++) {
			rte_pktmbuf_free(buf[i]);
		}
	}
}

/*
 * The main function, which does initialization and calls the per-lcore
 * functions.
 */
int
main(int argc, char *argv[])
{
	struct rte_mempool *mbuf_pool;
	uint16_t port = 0;

	/* Initialize the Environment Abstraction Layer (EAL). */
	int ret = rte_eal_init(argc, argv);
	if (ret < 0) {
		rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");
	}

	argc -= ret;
	argv += ret;

	/* Creates a new mempool in memory to hold the mbufs. */
	mbuf_pool = rte_pktmbuf_pool_create("MBUF_POOL", NUM_MBUFS,
		MBUF_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

	if (mbuf_pool == NULL)
		rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

	port_init(port, mbuf_pool);

	srand((unsigned) time(NULL));

	/* Call lcore_main on the master core only. */
	lcore_main(port, mbuf_pool);

	return 0;
}