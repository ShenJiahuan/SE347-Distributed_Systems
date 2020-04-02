#include <stdint.h>
#include "qos.h"
#include "rte_meter.h"
#include "rte_red.h"

#define COLOR_COUNT 3

static struct rte_meter_srtcm app_flows[APP_FLOWS_MAX];

static struct rte_meter_srtcm_params app_srtcm_params[] = {
    {.cir = 240000000, .cbs = 240000, .ebs = 240000},
    {.cir = 240000000, .cbs = 80000, .ebs = 80000},
    {.cir = 240000000, .cbs = 40000, .ebs = 40000},
    {.cir = 240000000, .cbs = 20000, .ebs = 20000}
};

// static struct rte_meter_srtcm_params app_srtcm_params[] = {
//     {.cir = 240000000, .cbs = 80000, .ebs = 80000},
//     {.cir = 240000000, .cbs = 80000, .ebs = 80000},
//     {.cir = 240000000, .cbs = 80000, .ebs = 80000},
//     {.cir = 240000000, .cbs = 80000, .ebs = 80000}
// };

static uint64_t cpu_freq;

static uint64_t start_time;

static struct rte_red app_reds[APP_FLOWS_MAX][COLOR_COUNT];

static struct rte_red_config app_red_configs[APP_FLOWS_MAX][COLOR_COUNT];

static struct rte_red_params app_red_params[][COLOR_COUNT] = {
    // flow 0
    {
        {.min_th = 511, .max_th = 512, .maxp_inv = 5, .wq_log2 = 5},  // green
        {.min_th = 10, .max_th = 11, .maxp_inv = 5, .wq_log2 = 5},    // yellow
        {.min_th = 1, .max_th = 2, .maxp_inv = 5, .wq_log2 = 5}       // red
    },
    // flow 1
    {
        {.min_th = 511, .max_th = 512, .maxp_inv = 5, .wq_log2 = 5},  // green
        {.min_th = 10, .max_th = 11, .maxp_inv = 5, .wq_log2 = 5},    // yellow
        {.min_th = 1, .max_th = 2, .maxp_inv = 5, .wq_log2 = 5}       // red
    },
    // flow 2
    {
        {.min_th = 511, .max_th = 512, .maxp_inv = 5, .wq_log2 = 5},  // green
        {.min_th = 10, .max_th = 11, .maxp_inv = 5, .wq_log2 = 5},    // yellow
        {.min_th = 1, .max_th = 2, .maxp_inv = 5, .wq_log2 = 5}       // red
    },
    // flow 3
    {
        {.min_th = 511, .max_th = 512, .maxp_inv = 1, .wq_log2 = 5},  // green
        {.min_th = 10, .max_th = 11, .maxp_inv = 1, .wq_log2 = 5},    // yellow
        {.min_th = 1, .max_th = 2, .maxp_inv = 1, .wq_log2 = 5}       // red
    }
};

// static struct rte_red_params app_red_params[][COLOR_COUNT] = {
//     // flow 0
//     {
//         {.min_th = 511, .max_th = 512, .maxp_inv = 10, .wq_log2 = 5}, // green
//         {.min_th = 511, .max_th = 512, .maxp_inv = 10, .wq_log2 = 5}, // yellow
//         {.min_th = 511, .max_th = 512, .maxp_inv = 10, .wq_log2 = 5}  // red
//     },
//     // flow 1
//     {
//         {.min_th = 256, .max_th = 512, .maxp_inv = 10, .wq_log2 = 5}, // green
//         {.min_th = 32, .max_th = 64, .maxp_inv = 5, .wq_log2 = 5},    // yellow
//         {.min_th = 1, .max_th = 2, .maxp_inv = 1, .wq_log2 = 5}       // red
//     },
//     // flow 2
//     {
//         {.min_th = 32, .max_th = 44, .maxp_inv = 5, .wq_log2 = 5},    // green
//         {.min_th = 4, .max_th = 8, .maxp_inv = 3, .wq_log2 = 5},      // yellow
//         {.min_th = 1, .max_th = 2, .maxp_inv = 1, .wq_log2 = 5}       // red
//     },
//     // flow 3
//     {
//         {.min_th = 12, .max_th = 18, .maxp_inv = 4, .wq_log2 = 5},    // green
//         {.min_th = 1, .max_th = 2, .maxp_inv = 2, .wq_log2 = 5},      // yellow
//         {.min_th = 1, .max_th = 2, .maxp_inv = 1, .wq_log2 = 5}       // red
//     }
// };

static int queue[APP_FLOWS_MAX];

static uint64_t last_burst = -1;

/**
 * Convert from nanosecond to cpu cycle
 */
static uint64_t get_time(uint64_t time_nano) {
    return time_nano * cpu_freq / 1000000000;
}

/**
 * This function will be called only once at the beginning of the test. 
 * You can initialize your meter here.
 * 
 * int rte_meter_srtcm_config(struct rte_meter_srtcm *m, struct rte_meter_srtcm_params *params);
 * @return: 0 upon success, error code otherwise
 * 
 * void rte_exit(int exit_code, const char *format, ...)
 * #define rte_panic(...) rte_panic_(__func__, __VA_ARGS__, "dummy")
 * 
 * uint64_t rte_get_tsc_hz(void)
 * @return: The frequency of the RDTSC timer resolution
 * 
 * static inline uint64_t rte_get_tsc_cycles(void)
 * @return: The time base for this lcore.
 */
int
qos_meter_init(void)
{
    cpu_freq = rte_get_tsc_hz();

    for (int i = 0; i < APP_FLOWS_MAX; i++) {
        int ret = rte_meter_srtcm_config(&app_flows[i], &app_srtcm_params[i]);
        if (ret != 0) {
            rte_exit(ret, "Error init qos meter\n");
        }
    }

    start_time = rte_get_tsc_cycles();
    return 0;
}

/**
 * This function will be called for every packet in the test, 
 * after which the packet is marked by returning the corresponding color.
 * 
 * A packet is marked green if it doesn't exceed the CBS, 
 * yellow if it does exceed the CBS, but not the EBS, and red otherwise
 * 
 * The pkt_len is in bytes, the time is in nanoseconds.
 * 
 * Point: We need to convert ns to cpu circles
 * Point: Time is not counted from 0
 * 
 * static inline enum rte_meter_color rte_meter_srtcm_color_blind_check(struct rte_meter_srtcm *m,
	uint64_t time, uint32_t pkt_len)
 * 
 * enum qos_color { GREEN = 0, YELLOW, RED };
 * enum rte_meter_color { e_RTE_METER_GREEN = 0, e_RTE_METER_YELLOW,  
	e_RTE_METER_RED, e_RTE_METER_COLORS };
 */ 
enum qos_color
qos_meter_run(uint32_t flow_id, uint32_t pkt_len, uint64_t time)
{
    return rte_meter_srtcm_color_blind_check(&app_flows[flow_id], get_time(time), pkt_len);
}


/**
 * This function will be called only once at the beginning of the test. 
 * You can initialize you dropper here
 * 
 * int rte_red_rt_data_init(struct rte_red *red);
 * @return Operation status, 0 success
 * 
 * int rte_red_config_init(struct rte_red_config *red_cfg, const uint16_t wq_log2, 
   const uint16_t min_th, const uint16_t max_th, const uint16_t maxp_inv);
 * @return Operation status, 0 success 
 */
int
qos_dropper_init(void)
{
    for (int i = 0; i < APP_FLOWS_MAX; i++) {
        for (int j = 0; j < COLOR_COUNT; j++) {
            int ret = rte_red_rt_data_init(&app_reds[i][j]);
            if (ret != 0) {
                rte_exit(ret, "Error init qos dropper\n");
            }

            uint16_t wq_log2 = app_red_params[i][j].wq_log2;
            uint16_t min_th = app_red_params[i][j].min_th;
            uint16_t max_th = app_red_params[i][j].max_th;
            uint16_t maxp_inv = app_red_params[i][j].maxp_inv;

            ret = rte_red_config_init(&app_red_configs[i][j], wq_log2, min_th, max_th, maxp_inv);
            if (ret != 0) {
                rte_exit(ret, "Error init qos dropper\n");
            }
        }
    }
    return 0;
}

/**
 * This function will be called for every tested packet after being marked by the meter, 
 * and will make the decision whether to drop the packet by returning the decision (0 pass, 1 drop)
 * 
 * The probability of drop increases as the estimated average queue size grows
 * 
 * static inline void rte_red_mark_queue_empty(struct rte_red *red, const uint64_t time)
 * @brief Callback to records time that queue became empty
 * @param q_time : Start of the queue idle time (q_time) 
 * 
 * static inline int rte_red_enqueue(const struct rte_red_config *red_cfg,
	struct rte_red *red, const unsigned q, const uint64_t time)
 * @param q [in] updated queue size in packets   
 * @return Operation status
 * @retval 0 enqueue the packet
 * @retval 1 drop the packet based on max threshold criteria
 * @retval 2 drop the packet based on mark probability criteria
 */
int
qos_dropper_run(uint32_t flow_id, enum qos_color color, uint64_t time)
{
    if (time != last_burst) {
        last_burst = time;
        for (int i = 0; i < APP_FLOWS_MAX; i++) {
            queue[i] = 0;
            for (int j = 0; j < COLOR_COUNT; j++) {
                rte_red_mark_queue_empty(&app_reds[i][j], get_time(time));
            }
        }
    }

    if (rte_red_enqueue(&app_red_configs[flow_id][color], &app_reds[flow_id][color], queue[flow_id], get_time(time)) == 0) {
        queue[flow_id]++;
        return 0;
    }
    return 1;
}
