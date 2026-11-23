//
// Created by victor on 5/14/25.
//

#ifndef OFFS_NODE_H
#define OFFS_NODE_H

#include "../Configuration/config.h"
#include "../Network/authority.h"
#include "../Network/network.h"
#include "../Scheduler/scheduler.h"
#include "../Timer/timer_actor.h"
#include "../Util/atomic_compat.h"
#include <stdint.h>
#include <stddef.h>

#define OFFS_ERROR_DRAINING -2

typedef struct block_cache_t block_cache_t;
typedef struct http_server_t http_server_t;

typedef struct offs_node_t {
  config_t* config;
  authority_t* authority;
  network_t* network;
  block_cache_t* block_cache;
  http_server_t* http_server;
  scheduler_pool_t* scheduler;
  timer_actor_t* timer;
  ATOMIC(uint8_t) running;
  ATOMIC(uint8_t) draining;
  uint64_t start_time_ms;
} offs_node_t;

offs_node_t* offs_node_create(config_t* config, authority_t* authority);
int offs_node_start(offs_node_t* node);
void offs_node_stop(offs_node_t* node);
void offs_node_stop_http(offs_node_t* node);
void offs_node_stop_network(offs_node_t* node);
void offs_node_destroy(offs_node_t* node);

/* Trigger a graceful restart using pending config.
   Must only be called from a local transport handler. */
void offs_node_restart(offs_node_t* node, const char* data_dir);

/* Live-apply peer rate-limit overrides from a staged config JSON object
   (the parsed body of a config PUT — a cJSON object, passed opaque to keep
   cJSON out of this header). Recognized keys: rate_<type> and
   rate_<type>_burst — see config.h CONFIG_RATE_TYPE_COUNT ordering
   (find_block, store_block, seeking_blocks, ping_capacity, ping). Values
   <= 0 keep the compiled default for that type. Returns the number of fields
   applied, or -1 when a rate field carries an invalid value (a
   rate_-prefixed field that is not a number or is negative); nothing is
   applied in that case. Does NOT mutate node->config — persistence rides
   the pending-config path so a restart reproduces the same values. */
int offs_node_apply_rate_configs_from_json(offs_node_t* node,
                                           const void* json_object);

#endif // OFFS_NODE_H