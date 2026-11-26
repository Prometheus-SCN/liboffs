#include <gtest/gtest.h>
#include <future>
#include <chrono>
#include <cstdlib>
#include <atomic>
#include <thread>
#include <vector>

/* The block_cache fixtures below hardcode a temp location under "/tmp". Two
   problems flow from that: (1) parallel test processes (or `ctest -j`) collide
   on the same on-disk block cache directory, corrupting each other's
   sections/index files; (2) it makes the suite non-hermetic. OFFS_BC_TMP lets a
   caller redirect that base to a private directory per process. Falls back to
   "/tmp" so the default behavior is unchanged. */
static const char* bc_tmp_base() {
  const char* t = getenv("OFFS_BC_TMP");
  return (t != NULL && t[0] != '\0') ? t : "/tmp";
}
extern "C" {
#include "../src/BlockCache/block.h"
#include "../src/BlockCache/block_cache.h"
#include "../src/BlockCache/sections.h"
#include "../src/Util/path_join.h"
#include "../src/Util/mkdir_p.h"
#include "../src/Util/rm_rf.h"
#include "../src/Configuration/config.h"
#include "../src/Timer/timer_actor.h"
#include "../src/Actor/actor.h"
#include "../src/Actor/message.h"
#include "../src/Scheduler/scheduler.h"
#include "../src/Util/atomic_compat.h"
#include "../src/Platform/platform_time.h"
#include <cbor.h>
#include "../src/Util/allocator.h"
}

/* ---- Completion actor for async block_cache tests ---- */

typedef struct {
  ATOMIC(uint8_t) done;
  int put_result;
  block_t* get_block;
  buffer_t* get_hash;
  int remove_result;
} bc_completion_t;

static void bc_completion_dispatch(void* state, message_t* msg) {
  bc_completion_t* cs = (bc_completion_t*)state;
  switch (msg->type) {
    case CACHE_PUT_RESULT: {
      cache_put_result_payload_t* r = (cache_put_result_payload_t*)msg->payload;
      cs->put_result = r->result;
      break;
    }
    case CACHE_GET_RESULT: {
      cache_get_result_payload_t* r = (cache_get_result_payload_t*)msg->payload;
      cs->get_block = r->block;
      cs->get_hash = r->hash;
      r->block = NULL;
      r->hash = NULL;
      break;
    }
    case CACHE_REMOVE_RESULT: {
      cache_remove_result_payload_t* r = (cache_remove_result_payload_t*)msg->payload;
      cs->remove_result = r->result;
      break;
    }
    default:
      break;
  }
  ATOMIC_STORE(&cs->done, 1);
}

/* Helper: put a block and wait for result */
static int bc_put_sync(block_cache_t* bc, block_t* block, scheduler_pool_t* pool) {
  bc_completion_t cs;
  memset(&cs, 0, sizeof(cs));
  actor_t comp;
  actor_init(&comp, &cs, bc_completion_dispatch, pool);

  block_t* ref_block = (block_t*)refcounter_reference((refcounter_t*)block);
  refcounter_yield((refcounter_t*)ref_block);
  block_cache_put(bc, ref_block, 0, &comp);

  if (pool) {
    /* comp is scheduled by actor_send when bc->actor delivers the result —
       do NOT pre-inject it with an empty queue (that spawns a spurious no-op
       run and a double-injection that races actor_destroy). */
    while (!ATOMIC_LOAD(&cs.done)) { platform_sleep_ms(1); }
    /* Wait for every pool worker to be idle before destroying the actor: a
       worker may still be in the tail of actor_run(&comp) (between setting
       cs.done and clearing ACTOR_FLAG_RUNNING), and actor_destroy must not
       free the queue out from under it. This mirrors block_cache_destroy /
       sections_destroy, which barrier on wait_for_idle before destroying their
       pool-backed actors. */
    scheduler_pool_wait_for_idle(pool);
  } else {
    while (!ATOMIC_LOAD(&cs.done)) {
      actor_run(&bc->actor, ACTOR_BATCH_SIZE);
      actor_run(&comp, ACTOR_BATCH_SIZE);
    }
  }

  actor_destroy(&comp);
  return cs.put_result;
}

/* Helper: get a block and wait for result */
static block_t* bc_get_sync(block_cache_t* bc, buffer_t* hash, scheduler_pool_t* pool) {
  bc_completion_t cs;
  memset(&cs, 0, sizeof(cs));
  actor_t comp;
  actor_init(&comp, &cs, bc_completion_dispatch, pool);

  block_cache_get(bc, hash, &comp);

  if (pool) {
    /* comp is scheduled by actor_send when bc->actor delivers the result —
       do NOT pre-inject it with an empty queue (that spawns a spurious no-op
       run and a double-injection that races actor_destroy). */
    while (!ATOMIC_LOAD(&cs.done)) { platform_sleep_ms(1); }
    /* Wait for every pool worker to be idle before destroying the actor: a
       worker may still be in the tail of actor_run(&comp) (between setting
       cs.done and clearing ACTOR_FLAG_RUNNING), and actor_destroy must not
       free the queue out from under it. This mirrors block_cache_destroy /
       sections_destroy, which barrier on wait_for_idle before destroying their
       pool-backed actors. */
    scheduler_pool_wait_for_idle(pool);
  } else {
    while (!ATOMIC_LOAD(&cs.done)) {
      actor_run(&bc->actor, ACTOR_BATCH_SIZE);
      actor_run(&comp, ACTOR_BATCH_SIZE);
    }
  }

  if (cs.get_hash) {
    DESTROY(cs.get_hash, buffer);
  }

  actor_destroy(&comp);
  return cs.get_block;
}

/* Helper: remove a block and wait for result */
static int bc_remove_sync(block_cache_t* bc, buffer_t* hash, scheduler_pool_t* pool) {
  bc_completion_t cs;
  memset(&cs, 0, sizeof(cs));
  actor_t comp;
  actor_init(&comp, &cs, bc_completion_dispatch, pool);

  block_cache_remove(bc, hash, &comp);

  if (pool) {
    /* comp is scheduled by actor_send when bc->actor delivers the result —
       do NOT pre-inject it with an empty queue (that spawns a spurious no-op
       run and a double-injection that races actor_destroy). */
    while (!ATOMIC_LOAD(&cs.done)) { platform_sleep_ms(1); }
    /* Wait for every pool worker to be idle before destroying the actor: a
       worker may still be in the tail of actor_run(&comp) (between setting
       cs.done and clearing ACTOR_FLAG_RUNNING), and actor_destroy must not
       free the queue out from under it. This mirrors block_cache_destroy /
       sections_destroy, which barrier on wait_for_idle before destroying their
       pool-backed actors. */
    scheduler_pool_wait_for_idle(pool);
  } else {
    while (!ATOMIC_LOAD(&cs.done)) {
      actor_run(&bc->actor, ACTOR_BATCH_SIZE);
      actor_run(&comp, ACTOR_BATCH_SIZE);
    }
  }

  actor_destroy(&comp);
  return cs.remove_result;
}

#ifdef _WIN32
#include <windows.h>

/* Isolate the per-restart OS-handle leak to block_cache. offs_node_restart
   destroys and re-creates the block_cache every cycle, so each cycle must
   release every OS handle it allocates (section/index/WAL file handles, any
   internal event/thread). The scheduler_pool + timer_actor are created once
   outside the loop (both handle-balanced per their own isolation tests). */
static DWORD bc_handle_count() {
  DWORD count = 0;
  GetProcessHandleCount(GetCurrentProcess(), &count);
  return count;
}

TEST(TestBlockCacheHandleLeak, TestBlockCacheCreateDestroyNoHandleLeak) {
  const char* base = bc_tmp_base();
  char* location = path_join(base, "BlockCacheHandleLeakTest");
  rm_rf(location);
  mkdir_p(location);

  scheduler_pool_t* pool = scheduler_pool_create(4);
  ASSERT_NE(pool, nullptr);
  scheduler_pool_start(pool);
  timer_actor_t* timer_actor = timer_actor_create(pool);
  ASSERT_NE(timer_actor, nullptr);

  config_t config = config_default();
  config.index_wait = 60000;
  config.index_max_wait = 60000;

  /* Warm up: first cycle may cache runtime/file handles; discard for a steady
     baseline. */
  for (int i = 0; i < 2; i++) {
    block_cache_t* bc = block_cache_create(config, location, standard, timer_actor, pool, NULL, 0);
    ASSERT_NE(bc, nullptr);
    block_cache_sync(bc);
    block_cache_destroy(bc);
    scheduler_pool_wait_for_idle(pool);
  }

  DWORD before = bc_handle_count();
  const int N = 20;
  for (int i = 0; i < N; i++) {
    block_cache_t* bc = block_cache_create(config, location, standard, timer_actor, pool, NULL, 0);
    ASSERT_NE(bc, nullptr);
    block_cache_sync(bc);
    block_cache_destroy(bc);
    scheduler_pool_wait_for_idle(pool);
  }
  DWORD after = bc_handle_count();

  timer_actor_destroy(timer_actor);
  scheduler_pool_wait_for_idle(pool);
  scheduler_pool_stop(pool);
  scheduler_pool_destroy(pool);
  rm_rf(location);
  free(location);

  LONG delta = (LONG)after - (LONG)before;
  EXPECT_LE(delta, 2) << "handle leak: +" << delta << " over " << N
                      << " block_cache cycles (before=" << before
                      << " after=" << after << ")";
}
#endif

class TestBlockLRU : public testing::Test {
public:
  size_t size = 5;
  size_t overage = 3;
  block_t* blocks[25];
  index_entry_t* entries[25];
  block_size_e block_type = mini;
  void SetUp() override {
    for (size_t i = 0; i < 25; i++) {
      blocks[i] = block_create_random_block_by_type(block_type);
      entries[i] = index_entry_create(blocks[i]->hash);
    }
  }
  void TearDown() override {
    for (size_t i = 0; i < 25; i++) {
      index_entry_destroy(entries[i]);
      block_destroy(blocks[i]);
    }
  }
};

TEST_F(TestBlockLRU, TestBlockLRUOperations) {
  block_lru_cache_t* lru = block_lru_cache_create(size);
  for (size_t i = 0; i < (size + overage); i++) {
    index_entry_t* ejected = (index_entry_t*)refcounter_reference(
        (refcounter_t*)block_lru_cache_put(lru, blocks[i], entries[i]));
    if (i >= size) {
      EXPECT_NE(ejected, nullptr);
    }
    if (ejected) {
      index_entry_destroy(ejected);
    }
  }
  for (size_t i = 0; i < overage; i++) {
    block_t* block = block_lru_cache_get(lru, blocks[i]->hash);
    EXPECT_EQ(block, nullptr);
    if (block) {
      block_destroy(block);
    }
  }
  for (size_t i = overage; i < (size + overage); i++) {
    block_t* block = block_lru_cache_get(lru, blocks[i]->hash);
    EXPECT_NE(block, nullptr);
    if (block) {
      block_destroy(block);
    }
  }
  for (size_t i = overage; i < (size + overage); i++) {
    EXPECT_EQ(block_lru_cache_contains(lru, blocks[i]->hash), true);
  }
  for (size_t i = overage; i < (size + overage); i++) {
    block_lru_cache_delete(lru, blocks[i]->hash);
  }
  for (size_t i = overage; i < (size + overage); i++) {
    EXPECT_EQ(block_lru_cache_contains(lru, blocks[i]->hash), false);
  }
  block_lru_cache_destroy(lru);
}

#define BLOCK_COUNT 25

class TestBlockCache : public testing::Test {
public:
  block_size_e type = standard;
  char* location;
  timer_actor_t* timer_actor;
  scheduler_pool_t* pool;
  block_cache_t* block_cache;
  block_t* blocks[BLOCK_COUNT];
  config_t config;
  void SetUp() override {
    location = path_join(bc_tmp_base(), "BlockCacheTest");
    rm_rf(location);
    pool = scheduler_pool_create(4);
    scheduler_pool_start(pool);
    timer_actor = timer_actor_create(pool);
    mkdir_p(location);
    config = config_default();
    /* Use a long debounce window so the 5ms timer cannot fire mid-test.
       The TearDown's explicit flush + sync handles persistence. */
    config.index_wait = 60000;
    config.index_max_wait = 60000;
    for (size_t i = 0; i < BLOCK_COUNT; i++) {
      blocks[i] = block_create_random_block_by_type(type);
    }
  }
  void TearDown() override {
    scheduler_pool_wait_for_idle(pool);
    if (block_cache != NULL) {
      block_cache_sync(block_cache);
    }
    block_cache_destroy(block_cache);
    timer_actor_destroy(timer_actor);
    scheduler_pool_stop(pool);
    scheduler_pool_destroy(pool);
    free(location);
    for (size_t i = 0; i < BLOCK_COUNT; i++) {
      block_destroy(blocks[i]);
    }
  }
};

TEST_F(TestBlockCache, TestBlockCache) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);

  /* Put all blocks */
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    int result = bc_put_sync(block_cache, blocks[i], pool);
    EXPECT_EQ(result, CACHE_PUT_NEW) << "Failed to store block " << i;
  }

  EXPECT_EQ(block_cache_count(block_cache), BLOCK_COUNT);

  if (HasFailure()) {
    GTEST_SKIP();
  }

  /* Re-put same blocks (should return CACHE_PUT_EXISTS, no duplicate) */
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    int result = bc_put_sync(block_cache, blocks[i], pool);
    EXPECT_EQ(result, CACHE_PUT_EXISTS) << "Re-put should return CACHE_PUT_EXISTS for block " << i;
  }

  EXPECT_EQ(block_cache_count(block_cache), BLOCK_COUNT);

  if (HasFailure()) {
    GTEST_SKIP();
  }

  /* Get all blocks */
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    block_t* block = bc_get_sync(block_cache, blocks[i]->hash, pool);
    EXPECT_NE(block, nullptr) << "Failed to retrieve block " << i;
    if (block != NULL) {
      EXPECT_EQ(buffer_compare(block->hash, blocks[i]->hash), 0);
      EXPECT_EQ(buffer_compare(block->data, blocks[i]->data), 0);
      block_destroy(block);
    }
  }

  if (HasFailure()) {
    GTEST_SKIP();
  }

  /* Remove all blocks */
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    int result = bc_remove_sync(block_cache, blocks[i]->hash, pool);
    EXPECT_EQ(result, 0) << "Failed to remove block " << i;
  }

  EXPECT_EQ(block_cache_count(block_cache), 0u);

  /* Get removed blocks (should return NULL) */
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    block_t* block = bc_get_sync(block_cache, blocks[i]->hash, pool);
    EXPECT_EQ(block, nullptr) << "Retrieved removed block " << i;
    if (block != NULL) {
      block_destroy(block);
    }
  }
}

TEST_F(TestBlockCache, TestBlockCachePutOnly) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    int result = bc_put_sync(block_cache, blocks[i], pool);
    EXPECT_EQ(result, CACHE_PUT_NEW);
  }
  EXPECT_EQ(block_cache_count(block_cache), BLOCK_COUNT);
}


/* ---- CACHE_LIST_SERVABLE: top-K offer list ---- */

typedef struct ls_capture_s {
  ATOMIC(uint8_t) done;
  cache_list_servable_result_payload_t* result;
} ls_capture_t;

static void _ls_capture_dispatch(void* state, message_t* msg) {
  ls_capture_t* capture = (ls_capture_t*)state;
  if (msg->type == CACHE_LIST_SERVABLE_RESULT) {
    capture->result = (cache_list_servable_result_payload_t*)msg->payload;
    /* CONSUME: actor_run's post-dispatch cleanup must not double-free. */
    msg->payload = NULL;
    msg->payload_destroy = NULL;
  }
  atomic_store(&capture->done, 1);
}

static cache_list_servable_result_payload_t* ls_servable_sync(
    block_cache_t* bc, size_t max_offers, const uint8_t* exclude,
    size_t exclude_count, scheduler_pool_t* pool) {
  static ATOMIC(uint64_t) ls_message_id = ATOMIC_VAR_INIT(1);
  ls_capture_t capture;
  atomic_store(&capture.done, 0);
  capture.result = NULL;

  actor_t comp;
  actor_init(&comp, &capture, _ls_capture_dispatch, pool);

  uint64_t message_id = atomic_fetch_add(&ls_message_id, 1) + 1;
  block_cache_list_servable(bc, max_offers, exclude, exclude_count,
                            message_id, &comp);

  if (pool != NULL) {
    while (!atomic_load(&capture.done)) {
      platform_sleep_ms(1);
    }
    scheduler_pool_wait_for_idle(pool);
  } else {
    while (!atomic_load(&capture.done)) {
      actor_run(&bc->actor, ACTOR_BATCH_SIZE);
      actor_run(&comp, ACTOR_BATCH_SIZE);
    }
  }
  actor_destroy(&comp);
  return capture.result;
}

/* Correctness: FIB-descending top-K over servable entries; excluded and
   ephemeral entries never offered. */
TEST_F(TestBlockCache, TestBlockCacheListServableTopKAndExclusions) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);

  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    EXPECT_EQ(bc_put_sync(block_cache, blocks[i], pool), CACHE_PUT_NEW);
  }
  /* Distinct FIB per entry: entry i gets fib i+1 (put counters reset below). */
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    index_entry_t* entry =
        index_get(block_cache->index, blocks[i]->hash);
    ASSERT_NE(entry, nullptr);
    entry->counter = fibonacci_hit_counter_from(i + 1, 1);
  }
  /* Two ephemeral entries drop out of the servable set. */
  index_entry_t* ephemeral_low = index_get(block_cache->index, blocks[0]->hash);
  index_entry_t* ephemeral_high = index_get(block_cache->index, blocks[BLOCK_COUNT - 1]->hash);
  ASSERT_NE(ephemeral_low, nullptr);
  ASSERT_NE(ephemeral_high, nullptr);
  ephemeral_low->ephemeral_count = 3;
  ephemeral_high->ephemeral_count = 3;

  /* Exclude the two highest surviving fibs (BLOCK_COUNT-1, BLOCK_COUNT-2). */
  uint8_t exclude[2 * 32];
  memcpy(exclude, blocks[BLOCK_COUNT - 2]->hash->data, 32);
  memcpy(exclude + 32, blocks[BLOCK_COUNT - 3]->hash->data, 32);

  cache_list_servable_result_payload_t* result =
      ls_servable_sync(block_cache, 20, exclude, 2, pool);
  ASSERT_NE(result, nullptr);

  const size_t expected_count = 20;
  ASSERT_NE(result->hashes, nullptr);
  ASSERT_EQ(result->offer_count, expected_count);

  /* Strictly descending fibs, and the excluded sets never appear. The
     highest servable non-excluded fib is BLOCK_COUNT-4 (indices 22.21 excluded. */
  for (size_t rank = 0; rank < result->offer_count; rank++) {
    uint32_t expected_fib = (uint32_t)(BLOCK_COUNT - 3 - rank);
    EXPECT_EQ(result->fibs[rank], expected_fib) << "rank " << rank;
    for (size_t ex = 0; ex < 2; ex++) {
      EXPECT_NE(memcmp(result->hashes + rank * 32, exclude + ex * 32, 32), 0);
    }
    EXPECT_NE(memcmp(result->hashes + rank * 32,
                     blocks[BLOCK_COUNT - 1]->hash->data, 32), 0);
    EXPECT_NE(memcmp(result->hashes + rank * 32, blocks[0]->hash->data, 32), 0);
  }

  DESTROY(result, cache_list_servable_result_payload);
}

/* Bounded runtime: the offer list is a TOP-K selection — the previous full
   O(n^2) bubble sort made a single reply take seconds on a 60k index and
   starved consumer cache lookups behind the seek flood (azure 10MB load,
   2026-11-26). 60k entries must answer within 5s. */
TEST_F(TestBlockCache, TestBlockCacheListServableScalesLinear) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);

  const size_t big_count = 60000;
  for (size_t i = 0; i < big_count; i++) {
    /* 32 distinct bytes: the 8-byte counter, then its byte-wise repeat. */
    uint8_t raw[32];
    memset(raw, 0, sizeof(raw));
    memcpy(raw, &i, sizeof(i));
    memcpy(raw + 16, &i, sizeof(i));
    buffer_t* hash_buf = buffer_create_from_pointer_copy(raw, 32);
    index_entry_t* entry = index_entry_from(
        hash_buf, 0, 0, 0, fibonacci_hit_counter_from((uint64_t)(i + 1), 1), 0, 0);
    buffer_destroy(hash_buf);
    index_add(block_cache->index, entry);
  }
  EXPECT_EQ(index_count(block_cache->index), big_count);

  const long long start_ms = (long long)(time(NULL) * 1000LL);
  cache_list_servable_result_payload_t* result =
      ls_servable_sync(block_cache, 20, NULL, 0, pool);
  const long long elapsed_ms = (long long)(time(NULL) * 1000LL) - start_ms;

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->offer_count, 20u);
  /* Top-K: the highest fibs land first (60000, 59999, ...). */
  EXPECT_EQ(result->fibs[0], 60000u);
  EXPECT_EQ(result->fibs[19], 59981u);

  EXPECT_LT(elapsed_ms, 5000) << "list-servable took " << elapsed_ms << "ms";

  DESTROY(result, cache_list_servable_result_payload);
}


/* Crash-recovery split (OFFS-275): the index snapshot+WAL and the sections'
   free-map meta are restored from independent persistence timelines. A
   SIGKILL between their flushes leaves a slot marked FREE in the recovered
   free map while an index entry still claims it; the next write then
   re-allocated the slot, double-claiming it (15 such slots in one archived
   azure index). Reconciliation at cache create must re-occupy every live
   index claim before any new write allocates. */
TEST_F(TestBlockCache, TestBlockCacheSlotReconciliationAfterCrashSplit) {
  /* One slot per section makes the allocator deterministic: the flipped
     slot in A's section is the ONLY free slot a post-crash write could
     take. */
  config.section_size = 1;

  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);
  ASSERT_NE(block_cache, nullptr);

  EXPECT_EQ(bc_put_sync(block_cache, blocks[0], pool), CACHE_PUT_NEW);
  index_entry_t* entry_a = index_get(block_cache->index, blocks[0]->hash);
  ASSERT_NE(entry_a, nullptr);
  size_t a_sid = entry_a->section_id;
  size_t a_idx = entry_a->section_index;

  /* Persist everything (index snapshot + section meta). */
  block_cache_sync(block_cache);
  block_cache_destroy(block_cache);
  block_cache = NULL;
  scheduler_pool_wait_for_idle(pool);

  /* Simulate the crash split: the section meta on disk still shows the
     slot FREE (its flush lost the race against SIGKILL). */
  char meta_path[1024];
  snprintf(meta_path, sizeof(meta_path), "%s/blocks/meta/%zu", location, a_sid);
  FILE* meta = fopen(meta_path, "r+b");
  ASSERT_NE(meta, nullptr);
  uint8_t meta_bytes[512];
  size_t meta_len = fread(meta_bytes, 1, sizeof(meta_bytes), meta);
  ASSERT_GE(meta_len, 5u);
  size_t byte_offset = 4 + a_idx / 8;
  ASSERT_LT(byte_offset, meta_len);
  meta_bytes[byte_offset] |= (uint8_t)(1 << (a_idx % 8)); /* 1 = free */
  fseek(meta, 0, SEEK_SET);
  fwrite(meta_bytes, 1, meta_len, meta);
  fclose(meta);

  /* Recreate: reconciliation must re-occupy the claimed slot. */
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);
  ASSERT_NE(block_cache, nullptr);

  section_t* section_a = sections_lru_cache_get(block_cache->sections->lru, a_sid);
  ASSERT_NE(section_a, nullptr);
  EXPECT_EQ(section_slot_occupied(section_a, a_idx), 1)
      << "index-claimed slot left FREE after crash-split restore";

  /* The next write must NOT reuse the still-claimed slot. */
  ASSERT_EQ(bc_put_sync(block_cache, blocks[1], pool), CACHE_PUT_NEW);
  index_entry_t* entry_b = index_get(block_cache->index, blocks[1]->hash);
  ASSERT_NE(entry_b, nullptr);
  EXPECT_FALSE(entry_b->section_id == a_sid && entry_b->section_index == a_idx)
      << "block B re-allocated block A's still-claimed slot (double claim)";

  /* A's data is untouched. */
  block_t* got_a = bc_get_sync(block_cache, blocks[0]->hash, pool);
  ASSERT_NE(got_a, nullptr);
  EXPECT_EQ(buffer_compare(got_a->data, blocks[0]->data), 0);
  block_destroy(got_a);
}

TEST_F(TestBlockCache, TestPutGroupAbortDeletesCreatedBlocks) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);

  /* Fresh puts under one abort group; the cache records its receipt. */
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    block_cache_put_group(block_cache, blocks[i], 0, nullptr, 77u);
  }
  scheduler_pool_wait_for_idle(pool);
  EXPECT_EQ(block_cache_count(block_cache), BLOCK_COUNT);

  /* Aborted PUT: END abort=1 force-deletes the group's fresh creations
     regardless of pin status. */
  block_cache_put_group_end(block_cache, 77u, 1);
  scheduler_pool_wait_for_idle(pool);
  EXPECT_EQ(block_cache_count(block_cache), 0u);

  /* Deleted blocks no longer read. */
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    block_t* got = bc_get_sync(block_cache, blocks[i]->hash, pool);
    EXPECT_EQ(got, nullptr) << "deleted block still readable for block " << i;
    if (got != nullptr) block_destroy(got);
  }
}

TEST_F(TestBlockCache, TestPutGroupAbortIgnoresPins) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);
  block_cache_put_group(block_cache, blocks[0], 0, nullptr, 79u);
  scheduler_pool_wait_for_idle(pool);
  /* A pinned block is still deleted by the abort (the user-facing rule). */
  block_cache_pin(block_cache, blocks[0]->hash, nullptr);
  scheduler_pool_wait_for_idle(pool);
  block_cache_put_group_end(block_cache, 79u, 1);
  scheduler_pool_wait_for_idle(pool);
  EXPECT_EQ(block_cache_count(block_cache), 0u);
}

TEST_F(TestBlockCache, TestPutGroupCompleteKeepsBlocks) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    block_cache_put_group(block_cache, blocks[i], 0, nullptr, 78u);
  }
  scheduler_pool_wait_for_idle(pool);
  /* Completed put: END abort=0 drops the receipt but keeps every block. */
  block_cache_put_group_end(block_cache, 78u, 0);
  scheduler_pool_wait_for_idle(pool);
  EXPECT_EQ(block_cache_count(block_cache), BLOCK_COUNT);
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    block_t* got = bc_get_sync(block_cache, blocks[i]->hash, pool);
    EXPECT_NE(got, nullptr);
    if (got != nullptr) block_destroy(got);
  }
}

TEST_F(TestBlockCache, TestPutGroupEndDropsLateStragglerPut) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);
  block_cache_put_group(block_cache, blocks[0], 0, nullptr, 90u);
  scheduler_pool_wait_for_idle(pool);
  block_cache_put_group_end(block_cache, 90u, 1);
  scheduler_pool_wait_for_idle(pool);
  EXPECT_EQ(block_cache_count(block_cache), 0u);

  /* A recipe-actor straggler arriving after the END (FIFO slip past the
     receipt sweep): a fresh creation for a tombstoned group is dropped, not
     stored, and must not re-create a ghost receipt. */
  block_cache_put_group(block_cache, blocks[0], 0, nullptr, 90u);
  scheduler_pool_wait_for_idle(pool);
  EXPECT_EQ(block_cache_count(block_cache), 0u);
  block_t* got = bc_get_sync(block_cache, blocks[0]->hash, pool);
  EXPECT_EQ(got, nullptr) << "post-END straggler block was stored";
  if (got != nullptr) block_destroy(got);
}

TEST_F(TestBlockCache, TestBlockCachePutGetOnly) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    int result = bc_put_sync(block_cache, blocks[i], pool);
    EXPECT_EQ(result, CACHE_PUT_NEW);
  }
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    block_t* block = bc_get_sync(block_cache, blocks[i]->hash, pool);
    EXPECT_NE(block, nullptr);
    if (block) block_destroy(block);
  }
  EXPECT_EQ(block_cache_count(block_cache), BLOCK_COUNT);
}

TEST_F(TestBlockCache, TestBlockCachePutRemoveOnly) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    int result = bc_put_sync(block_cache, blocks[i], pool);
    EXPECT_EQ(result, CACHE_PUT_NEW);
  }
  for (size_t i = 0; i < BLOCK_COUNT; i++) {
    int result = bc_remove_sync(block_cache, blocks[i]->hash, pool);
    EXPECT_EQ(result, 0);
  }
  EXPECT_EQ(block_cache_count(block_cache), 0u);
}

/* ---- Integration tests ---- */

#define LRU_COUNT 35

class TestBlockCacheIntegration : public testing::Test {
public:
  block_size_e type = standard;
  char* location;
  timer_actor_t* timer_actor;
  block_cache_t* block_cache;
  block_t* blocks[LRU_COUNT];
  config_t config;
  scheduler_pool_t* pool;
  void SetUp() override {
    location = path_join(bc_tmp_base(), "BlockCacheIntegrationTest");
    rm_rf(location);
    pool = scheduler_pool_create(4);
    scheduler_pool_start(pool);
    timer_actor = timer_actor_create(pool);
    mkdir_p(location);
    config = config_default();
    config.lru_size = 10;
    /* Use a long debounce window so the 5ms timer cannot fire mid-test. */
    config.index_wait = 60000;
    config.index_max_wait = 60000;
    for (size_t i = 0; i < LRU_COUNT; i++) {
      blocks[i] = block_create_random_block_by_type(type);
    }
  }
  void TearDown() override {
    scheduler_pool_wait_for_idle(pool);
    if (block_cache != NULL) {
      block_cache_sync(block_cache);
    }
    block_cache_destroy(block_cache);
    timer_actor_destroy(timer_actor);
    scheduler_pool_stop(pool);
    scheduler_pool_destroy(pool);
    free(location);
    for (size_t i = 0; i < LRU_COUNT; i++) {
      block_destroy(blocks[i]);
    }
  }
};

TEST_F(TestBlockCacheIntegration, TestLRUEjectionAndReload) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);

  /* Put more blocks than LRU can hold */
  for (size_t i = 0; i < LRU_COUNT; i++) {
    int result = bc_put_sync(block_cache, blocks[i], pool);
    EXPECT_EQ(result, CACHE_PUT_NEW) << "Failed to put block " << i;
  }

  /* All blocks should still be in the index (including ejected ones) */
  EXPECT_EQ(block_cache_count(block_cache), (size_t)LRU_COUNT);

  if (HasFailure()) {
    GTEST_SKIP();
  }

  /* Get all blocks — ejected ones should reload from sections */
  for (size_t i = 0; i < LRU_COUNT; i++) {
    block_t* block = bc_get_sync(block_cache, blocks[i]->hash, pool);
    EXPECT_NE(block, nullptr) << "Failed to retrieve block " << i;
    if (block != NULL) {
      EXPECT_EQ(buffer_compare(block->data, blocks[i]->data), 0)
          << "Data mismatch for block " << i;
      block_destroy(block);
    }
  }
}

TEST_F(TestBlockCacheIntegration, TestGetNonExistent) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);

  block_t* result = bc_get_sync(block_cache, blocks[0]->hash, pool);
  EXPECT_EQ(result, nullptr);

  /* Removing non-existent block should succeed (idempotent) */
  int rc = bc_remove_sync(block_cache, blocks[0]->hash, pool);
  EXPECT_EQ(rc, 0);
}

TEST_F(TestBlockCacheIntegration, TestPutDuplicate) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);

  /* Put same block twice — should not create duplicate index entry */
  int result = bc_put_sync(block_cache, blocks[0], pool);
  EXPECT_EQ(result, CACHE_PUT_NEW);

  result = bc_put_sync(block_cache, blocks[0], pool);
  EXPECT_EQ(result, CACHE_PUT_EXISTS);

  EXPECT_EQ(block_cache_count(block_cache), 1u);
}

TEST_F(TestBlockCacheIntegration, TestRemoveIdempotent) {
  block_cache = block_cache_create(config, location, type, timer_actor, pool, NULL, 0);

  int result = bc_put_sync(block_cache, blocks[0], pool);
  EXPECT_EQ(result, CACHE_PUT_NEW);
  EXPECT_EQ(block_cache_count(block_cache), 1u);

  /* Remove once */
  result = bc_remove_sync(block_cache, blocks[0]->hash, pool);
  EXPECT_EQ(result, 0);
  EXPECT_EQ(block_cache_count(block_cache), 0u);

  /* Remove again — should be idempotent */
  result = bc_remove_sync(block_cache, blocks[0]->hash, pool);
  EXPECT_EQ(result, 0);
  EXPECT_EQ(block_cache_count(block_cache), 0u);
}

TEST_F(TestBlockCacheIntegration, TestMiniBlocks) {
  block_cache = block_cache_create(config, location, mini, timer_actor, pool, NULL, 0);

  for (size_t i = 0; i < 10; i++) {
    block_t* mini_block = block_create_random_block_by_type(mini);
    int result = bc_put_sync(block_cache, mini_block, pool);
    EXPECT_EQ(result, CACHE_PUT_NEW);
    block_t* retrieved = bc_get_sync(block_cache, mini_block->hash, pool);
    EXPECT_NE(retrieved, nullptr);
    if (retrieved) {
      EXPECT_EQ(buffer_compare(retrieved->data, mini_block->data), 0);
      block_destroy(retrieved);
    }
    block_destroy(mini_block);
  }
}

/* ---- block_cache_can_fit tests ---- */

class BlockCacheCanFit : public testing::Test {
public:
  char* location;
  timer_actor_t* timer_actor;
  scheduler_pool_t* pool;
  block_cache_t* block_cache;
  config_t config;
  void SetUp() override {
    location = path_join(bc_tmp_base(), "BlockCacheCanFitTest");
    rm_rf(location);
    pool = scheduler_pool_create(4);
    scheduler_pool_start(pool);
    timer_actor = timer_actor_create(pool);
    mkdir_p(location);
    config = config_default();
    config.index_wait = 60000;
    config.index_max_wait = 60000;
    block_cache = NULL;
  }
  void TearDown() override {
    scheduler_pool_wait_for_idle(pool);
    if (block_cache != NULL) {
      block_cache_sync(block_cache);
    }
    block_cache_destroy(block_cache);
    timer_actor_destroy(timer_actor);
    scheduler_pool_stop(pool);
    scheduler_pool_destroy(pool);
    free(location);
  }
};

TEST_F(BlockCacheCanFit, ReturnsOkWhenSpaceAvailable) {
  /* 1 MB cap, empty cache, 500 KB required: fits. */
  block_cache = block_cache_create(config, location, standard, timer_actor, pool, NULL, 1000000);
  ASSERT_NE(block_cache, nullptr);
  EXPECT_EQ(block_cache_can_fit(block_cache, 500000), CACHE_FIT_OK);
}

TEST_F(BlockCacheCanFit, ReturnsFullWhenExceedsCapacity) {
  /* 1 MB cap, empty cache, 2 MB required: exceeds cap. */
  block_cache = block_cache_create(config, location, standard, timer_actor, pool, NULL, 1000000);
  ASSERT_NE(block_cache, nullptr);
  EXPECT_EQ(block_cache_can_fit(block_cache, 2000000), CACHE_FIT_FULL);
}

TEST_F(BlockCacheCanFit, ReturnsOkWhenMaxCapacityZero) {
  /* max_capacity_bytes = 0 means disabled; always OK. */
  block_cache = block_cache_create(config, location, standard, timer_actor, pool, NULL, 0);
  ASSERT_NE(block_cache, nullptr);
  EXPECT_EQ(block_cache_can_fit(block_cache, 999999999ULL), CACHE_FIT_OK);
}

/* ---- sections_dispatch concurrency stress ----
 *
 * sections_dispatch runs on two actor threads concurrently in production: the
 * block_cache actor calls it synchronously (CACHE_PUT / sync CACHE_GET /
 * CACHE_DEALLOCATE) while the sections actor's worker calls it for async
 * sections_read/write/deallocate. Both mutate sections->lru and sections->robin
 * -> list/hashmap corruption, double section_create, concurrent
 * section_dispatch on one section -> on-disk corruption. This stress test
 * hammers the sync path from multiple threads at once; before the dispatch_lock
 * mutex this races and intermittently crashes/corrupts. With the mutex it must
 * pass 10/10. See docs/concurrency-pass.md F2. */
TEST(SectionsConcurrency, SyncDispatchFromMultipleThreadsDoesNotCorrupt) {
  const char* base = bc_tmp_base();
  char* location = path_join(base, "SectionsConcurrencyTest");
  rm_rf(location);
  mkdir_p(location);

  scheduler_pool_t* pool = scheduler_pool_create(2);
  ASSERT_NE(pool, nullptr);
  scheduler_pool_start(pool);
  timer_actor_t* timer_actor = timer_actor_create(pool);
  ASSERT_NE(timer_actor, nullptr);

  /* mini blocks are 64000 bytes; sections expects data->size == block_size. */
  const size_t block_data_size = (size_t)mini;
  const size_t cache_size = 8;
  const size_t section_size = 8;
  const size_t max_tuple_size = 8;
  sections_t* sections = sections_create(location, section_size, cache_size,
                                          max_tuple_size, mini, timer_actor,
                                          pool, /*wait=*/5, /*max_wait=*/5000,
                                          /*fsync_data=*/false);
  ASSERT_NE(sections, nullptr);

  const int thread_count = 4;
  std::atomic<bool> stop{false};
  std::atomic<long> sync_ops{0};

  auto worker = [&]() {
    /* Each thread owns its own buffer (read-only by section_dispatch, which
       pwrites from p->data->data; the buffer is not consumed). */
    buffer_t* data = buffer_create(block_data_size);
    ASSERT_NE(data, nullptr);
    memset(data->data, 0xAA, block_data_size);
    sections_write_payload_t payload;
    memset(&payload, 0, sizeof(payload));
    payload.data = data;
    payload.reply_to = NULL;  /* sync: no reply dispatched */
    message_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = SECTIONS_WRITE;
    msg.payload = &payload;
    msg.payload_destroy = NULL;
    while (!stop.load(std::memory_order_acquire)) {
      payload.result = -1;
      payload.section_id = 0;
      payload.section_index = 0;
      sections_dispatch(sections, &msg);
      sync_ops.fetch_add(1, std::memory_order_relaxed);
    }
    buffer_destroy(data);
  };

  std::vector<std::thread> threads;
  for (int thread_index = 0; thread_index < thread_count; thread_index++) {
    threads.emplace_back(worker);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  stop.store(true, std::memory_order_release);

  for (auto& thread : threads) thread.join();

  EXPECT_GT(sync_ops.load(), 0);

  scheduler_pool_wait_for_idle(pool);
  sections_destroy(sections);
  timer_actor_destroy(timer_actor);
  scheduler_pool_wait_for_idle(pool);
  scheduler_pool_stop(pool);
  scheduler_pool_destroy(pool);
  rm_rf(location);
  free(location);
}