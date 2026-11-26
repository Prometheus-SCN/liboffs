//
// Created by victor on 5/7/26.
//

#ifndef OFFS_READABLE_OFF_STREAM_H
#define OFFS_READABLE_OFF_STREAM_H

#include "../Streams/stream.h"
#include "tuple.h"
#include "tuple_cache.h"
#include "ori.h"
#include "../BlockCache/block_cache.h"

typedef struct network_t network_t;

typedef enum {
  OFF_STREAM_FETCHING_BLOCKS,      /* waiting for block_cache_get results */
  OFF_STREAM_AWAITING_NETWORK,     /* waiting for NETWORK_FIND_BLOCK_RESULT */
  OFF_STREAM_AWAITING_TUPLE,       /* waiting for next tuple */
} off_stream_state_e;

/* Tracks an in-progress block fetch for async decode */
typedef struct pending_block_fetch_t {
  buffer_t* hash;
  size_t index;
  struct pending_block_fetch_t* next;
  /* Non-NULL only in presatisfied entries (results that raced ahead of the
     fetch start): the delivered block, parked until _start_block_fetches
     XORs it in and destroys it. */
  block_t* carried_block;
} pending_block_fetch_t;

/* Queued tuple waiting to be processed */
typedef struct pending_tuple_t {
  tuple_t* tuple;
  struct pending_tuple_t* next;
} pending_tuple_t;

/* Concurrent tuple fetch window (OFFS-274): find round trips dominate the
   consumer path (~100ms per tuple serialized), so up to READ_STREAM_FETCH_
   WINDOW tuples may have lookups + block fetches in flight simultaneously.
   Completed tuples park in the in-flight FIFO until the render drain reaches
   them in stream order — renders (and sent_bytes accounting) stay strictly
   ordered for every transport. */
/* Window depth: find round trips are ~50-70ms and the transfer is
   RTT-pipelined, so depth scales throughput nearly linearly until the wire
   or the holder rate-limit saturates. 64 tuples ≈ 192 outstanding finds ≈
   one transfer's full tuple count; per-in-flight memory is ~4KB (state +
   one 32B hash per node) plus parked 128KB accumulators for completed
   tuples awaiting their render turn — bounded by the FIFO's ordinal order. */
#define READ_STREAM_FETCH_WINDOW 64

/* One tuple between arrival and render. Held in an ordinal-ordered in-flight
   FIFO (append on start, pop from head at render). */
typedef struct inflight_tuple_t {
  struct inflight_tuple_t* next;
  tuple_t* tuple;                     /* reference owned by this entry */
  size_t ordinal;                     /* write-order sequence */
  uint8_t in_lookup;                  /* tuple_cache_get outstanding */
  uint8_t completed;                  /* decoded; parked for in-order render */
  buffer_t* encoded_data;             /* decoded payload awaiting render turn */
  buffer_t* xor_accumulator;          /* block data received so far */
  size_t blocks_expected;
  size_t blocks_received;
  pending_block_fetch_t* pending_fetches;   /* unanswered hashes of this tuple */
  pending_block_fetch_t* presatisfied;      /* raced results parked pre-fetch */
} inflight_tuple_t;

/* Progress payload for load_tuple_event (load mode only). Refcounted: the
 * notify machinery holds references while dispatching, so handlers should
 * copy the two counters out or take their own reference. */
typedef struct {
  refcounter_t refcounter;
  size_t tuples_loaded;
  size_t tuples_skipped;
} load_tuple_payload_t;

load_tuple_payload_t* load_tuple_payload_create(size_t tuples_loaded, size_t tuples_skipped);
void load_tuple_payload_destroy(void* payload);

/* Block size in bytes for a given block type. Shared by the descriptor, the
 * off_stream and LOAD consumers that compute tuple totals from a file size. */
size_t off_block_size_for_type(block_size_e type);

typedef struct {
  stream_t stream;
  block_cache_t* bc;
  tuple_cache_t* tc;
  ori_t* ori;
  network_t* network;               /* NULL = local-only mode */
  size_t descriptor_pad;
  size_t sent_bytes;
  size_t offset_remainder;
  uint8_t offset_applied;
  uint8_t load_mode;                /* 1 = missing data tuples are skipped and tallied */
  uint8_t closed;                   /* 1 = close_event already notified (render or request_close) */
  size_t tuples_loaded;             /* tuples whose data was served / rendered */
  size_t tuples_skipped;            /* tuples abandoned because blocks were missing */
  off_stream_state_e state;         /* coarse stream state (diagnostic) */
  /* Windowed in-flight tuples, FIFO by ordinal: head = oldest (next to
     render), tail = newest. Completed entries park in place until the render
     drain reaches them in order. */
  inflight_tuple_t* inflight_head;
  inflight_tuple_t* inflight_tail;
  size_t inflight_count;
  size_t next_ordinal;              /* ordinal for the next started tuple */
  /* Hashes of already-skipped tuples that are STRICTLY UNANSWERED (answered
     fetches are pruned as their results are consumed, see
     readable_off_stream.c): late results matching these are dropped so they
     cannot pollute any in-flight tuple's XOR accumulator. */
  pending_block_fetch_t* stale_fetches;
  /* Queue of tuples waiting for a window slot */
  pending_tuple_t* tuple_queue;
} readable_off_stream_t;

/* Load mode: missing data tuples are skipped and tallied (tuples_skipped)
 * instead of deactivating the stream; each resolved tuple (loaded or skipped)
 * emits load_tuple_event with a load_tuple_payload_t. Descriptor misses
 * remain fatal. */
readable_off_stream_t* readable_off_stream_create_ex(
    scheduler_pool_t* pool, block_cache_t* bc, tuple_cache_t* tc,
    ori_t* ori, size_t descriptor_pad, network_t* network, uint8_t load_mode);

readable_off_stream_t* readable_off_stream_create(
    scheduler_pool_t* pool, block_cache_t* bc, tuple_cache_t* tc,
    ori_t* ori, size_t descriptor_pad, network_t* network);
void readable_off_stream_destroy(readable_off_stream_t* stream);
void readable_off_stream_dispatch(void* state, message_t* msg);
void readable_off_stream_write(readable_off_stream_t* stream, tuple_t* tuple);

/* Load mode: close the stream after the last tuple resolved (loaded+skipped
   reached the enumerator's total), when rendering cannot complete because
   skipped tuples never advance sent_bytes. Idempotent: no-op if already
   deactivated or closed. Performs the same cleanup as CLOSE_STREAM and
   notifies close_event (no error). */
void readable_off_stream_request_close(readable_off_stream_t* stream);

#endif //OFFS_READABLE_OFF_STREAM_H