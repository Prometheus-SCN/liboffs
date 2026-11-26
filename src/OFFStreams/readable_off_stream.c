//
// Created by victor on 5/7/26.
//

#include "readable_off_stream.h"
#include "../Util/allocator.h"
#include "../Util/error.h"
#include "../Actor/actor.h"
#include "../Actor/message.h"
#include "../Buffer/buffer.h"
#include "../Scheduler/scheduler.h"
#include "../Network/network.h"

static size_t _block_size_for_type(block_size_e type);

size_t off_block_size_for_type(block_size_e type) {
  switch (type) {
    case mega:     return 1000000;
    case standard: return 128000;
    case mini:     return 64000;
    case nano:     return 136;
  }
  return 128000;
}

static size_t _block_size_for_type(block_size_e type) {
  return off_block_size_for_type(type);
}

static void _render_origin_data(readable_off_stream_t* stream, buffer_t* data) {
  size_t start;
  size_t length;
  if (!stream->offset_applied && stream->offset_remainder > 0) {
    size_t available = data->size - stream->offset_remainder;
    if (stream->sent_bytes + available > stream->ori->final_byte) {
      length = stream->ori->final_byte - stream->sent_bytes;
    } else {
      length = available;
    }
    start = stream->offset_remainder;
    stream->offset_applied = 1;
  } else {
    if (stream->sent_bytes + data->size > stream->ori->final_byte) {
      length = stream->ori->final_byte - stream->sent_bytes;
    } else {
      length = data->size;
    }
    start = 0;
  }

  buffer_t* slice = buffer_slice(data, start, start + length);
  if (slice != NULL) {
    stream_notify((stream_t*)stream, data_event, CONSUME(slice, buffer_t), (void (*)(void*))buffer_destroy);
  }

  stream->sent_bytes += length;

  if (stream->sent_bytes >= stream->ori->final_byte) {
    /* Mark closed before notifying: the close notification is queued through
     * the actor mailbox, and consumers of an earlier load_tuple_event must
     * already observe the closed flag when they run request_close. */
    stream->closed = 1;
    stream_notify((stream_t*)stream, finished_event, NULL, NULL);
    stream_notify((stream_t*)stream, complete_event, NULL, NULL);
    stream_notify((stream_t*)stream, close_event, NULL, NULL);
  }
}

static void _drain_tuple_queue(readable_off_stream_t* stream);
static void _inflight_render_advance(readable_off_stream_t* stream);
static void _close_stream(readable_off_stream_t* stream);

load_tuple_payload_t* load_tuple_payload_create(size_t tuples_loaded, size_t tuples_skipped) {
  load_tuple_payload_t* payload = get_clear_memory(sizeof(load_tuple_payload_t));
  payload->tuples_loaded = tuples_loaded;
  payload->tuples_skipped = tuples_skipped;
  refcounter_init((refcounter_t*)payload);
  return payload;
}

void load_tuple_payload_destroy(void* payload) {
  load_tuple_payload_t* progress = (load_tuple_payload_t*)payload;
  if (progress == NULL) {
    return;
  }
  if (refcounter_dereference_is_zero((refcounter_t*)progress)) {
    free(progress);
  }
}

/* Emit the load-mode progress event for the just-resolved tuple (loaded or
 * skipped). Only emitted when load_mode is set. */
static void _notify_load_tuple(readable_off_stream_t* stream) {
  if (!stream->load_mode) {
    return;
  }
  load_tuple_payload_t* progress =
      load_tuple_payload_create(stream->tuples_loaded, stream->tuples_skipped);
  stream_notify((stream_t*)stream, load_tuple_event,
                CONSUME(progress, load_tuple_payload_t),
                load_tuple_payload_destroy);
}

/* Tuple-completion point: tally the resolved tuple and emit the counted
 * progress event. Must be called BEFORE any render/finished/complete/close
 * notification so consumers always observe the final tuple count before the
 * stream closes. */
static void _tally_tuple_loaded(readable_off_stream_t* stream) {
  stream->tuples_loaded++;
  _notify_load_tuple(stream);
}

/* Destroy every node of a fetch list (pending or stale), releasing the hash
 * reference each node holds. */
static void _clear_fetch_list(pending_block_fetch_t* head) {
  while (head != NULL) {
    pending_block_fetch_t* next = head->next;
    DESTROY(head->hash, buffer);
    free(head);
    head = next;
  }
}

/* Release every stale fetch hash parked by skipped tuples. */
/* Destroy every presatisfied entry, releasing the carried block (racing
   results parked before the fetch start) and the hash reference. */
static void _clear_presatisfied(readable_off_stream_t* stream) {
  /* Presatisfied lists are per-in-flight tuple now; the close path clears
     them per entry. Kept for stream-level teardown loops (see below). */
}

static void _clear_stale_fetches(readable_off_stream_t* stream) {
  _clear_fetch_list(stream->stale_fetches);
  stream->stale_fetches = NULL;
}

/* Unlink and destroy the pending fetch node matching `hash`, if any. Called at
 * the moment a block result is consumed for the live tuple, so an answered
 * fetch never lingers in pending_fetches. Only because of this prune does
 * _skip_pending_tuple park STRICTLY UNANSWERED hashes in stale_fetches — which
 * is what makes matching late results by hash sound (see _consume_stale_fetch).
 * A node's hash buffer is a private reference on the tuple's hash (taken by
 * _start_block_fetches), so destroying the node's reference never frees the
 * hash the tuple still owns. */
/* If the result hash belongs to a tuple that was already skipped, consume the
 * matching stale entry and report the late result as dropped. Late results for
 * a skipped tuple must never accumulate into the live tuple's XOR accumulator.
 * Correctness of the hash match: every CACHE_GET_RESULT carries result->hash
 * (block_cache.c sets it on the LRU hit, the index miss and the section-read
 * result paths), and every NETWORK_FIND_BLOCK_RESULT construction site sets
 * result->hash (network.c wanted-list notify, store/accept relay notify and
 * the timeout sweep all build the payload from a 32-byte wanted-list hash).
 * A direct-return network result therefore matches a stale entry the same way
 * a cache result does — no generation counter is needed.
 *
 * Duplicate-hash safety: after _prune_answered_fetch, a hash can appear in
 * BOTH this list and the live tuple's request set only when two requests for
 * it were issued and NEITHER has been answered yet. Both block_cache and the
 * network deliver exactly one reply per outstanding request (one reply per
 * pending_get_t in block_cache.c; the network's wanted-list sweep guarantees a
 * found=0 NETWORK_FIND_BLOCK_RESULT for every requester), so the abandoned
 * tuple's unanswered request and the next tuple's fresh request for the same
 * hash each get their own reply: the first reply drains the stale entry
 * (dropping one duplicate of identical content — same hash means same block
 * bytes), and the reply for the live request is processed normally. Both
 * replies deliver, so the live tuple's block count still completes. */
static uint8_t _consume_stale_fetch(readable_off_stream_t* stream, buffer_t* hash) {
  if (hash == NULL) {
    return 0;
  }
  pending_block_fetch_t** current = &stream->stale_fetches;
  while (*current != NULL) {
    pending_block_fetch_t* fetch_node = *current;
    if (buffer_compare(fetch_node->hash, hash) == 0) {
      *current = fetch_node->next;
      DESTROY(fetch_node->hash, buffer);
      free(fetch_node);
      return 1;
    }
    current = &fetch_node->next;
  }
  return 0;
}
/* ---- windowed in-flight machinery (OFFS-274) ---- */

/* Unlink every node of `list` and append it to `*stale` (skipped tuples'
   unanswered hashes absorb late answers). Nodes' hash references move with
   them. */
static void _park_nodes_as_stale(pending_block_fetch_t** stale, pending_block_fetch_t* list) {
  pending_block_fetch_t* node = list;
  while (node != NULL) {
    pending_block_fetch_t* next = node->next;
    node->next = *stale;
    *stale = node;
    node = next;
  }
}

/* Release a per-tuple or global presatisfied list: racing results that were
   folded at fetch start never sit here; entries that parked late (answered
   but unowned) destroy their carried blocks and hashes. */
static void _clear_presatisfied_list(pending_block_fetch_t* list) {
  while (list != NULL) {
    pending_block_fetch_t* next = list->next;
    if (list->carried_block != NULL) {
      DESTROY(list->carried_block, block);
    }
    DESTROY(list->hash, buffer);
    free(list);
    list = next;
  }
}

/* Does the tuple reference `hash`? (Shared across in-flight lookups: a
   hash can appear in more than one tuple's fetch set.) */
static uint8_t _tuple_contains_hash(const tuple_t* tuple, const buffer_t* hash) {
  if (tuple == NULL || hash == NULL) return 0;
  size_t count = tuple_size((tuple_t*)tuple);
  for (size_t index = 0; index < count; index++) {
    buffer_t* candidate = tuple_get((tuple_t*)tuple, index);
    if (candidate != NULL && buffer_compare(candidate, (buffer_t*)hash) == 0) {
      return 1;
    }
  }
  return 0;
}

/* Find the in-flight tuple holding an UNANSWERED fetch node for `hash`,
   prune it, and return the tuple — its accumulator folds the answer. With
   duplicate hashes across concurrently fetched tuples either assignment is
   correct: both gets produced replies carrying the same block data. */
static inflight_tuple_t* _inflight_prune_fetch(readable_off_stream_t* stream,
                                               buffer_t* hash) {
  if (hash == NULL) {
    return NULL;
  }
  inflight_tuple_t* current = stream->inflight_head;
  while (current != NULL) {
    pending_block_fetch_t** node = &current->pending_fetches;
    while (*node != NULL) {
      if (buffer_compare((*node)->hash, hash) == 0) {
        pending_block_fetch_t* matched = *node;
        *node = matched->next;
        DESTROY(matched->hash, buffer);
        free(matched);
        return current;
      }
      node = &(*node)->next;
    }
    current = current->next;
  }
  return NULL;
}

/* Find the first in-flight tuple whose tuple contains `hash` — the owner of
   a raced result (the fetch node is not yet issued: lookup stage) and the
   parked-result target. */
static inflight_tuple_t* _inflight_contains_hash(readable_off_stream_t* stream,
                                                 buffer_t* hash) {
  inflight_tuple_t* current = stream->inflight_head;
  while (current != NULL) {
    if (_tuple_contains_hash(current->tuple, hash)) {
      return current;
    }
    current = current->next;
  }
  return NULL;
}

static void _inflight_park_presatisfied(readable_off_stream_t* stream,
                                        inflight_tuple_t* owner, buffer_t* hash,
                                        block_t* block) {
  (void)stream;
  pending_block_fetch_t* parked = get_clear_memory(sizeof(pending_block_fetch_t));
  parked->hash = (buffer_t*)refcounter_reference((refcounter_t*)hash);
  parked->index = 0;
  parked->carried_block = block;
  parked->next = owner->presatisfied;
  owner->presatisfied = parked;
}

/* Complete an in-flight tuple: store the XOR in the tuple cache, tally the
   tuple (monotone cumulative counters — correct under out-of-order decode),
   then park the decoded payload for the in-order render drain. */
static void _inflight_complete(readable_off_stream_t* stream,
                               inflight_tuple_t* tuple) {
  if (stream->tc != NULL) {
    tuple_cache_put(stream->tc, tuple->tuple, tuple->xor_accumulator);
  }
  /* Tally deferred to the render pop: load events then always arrive in
     ordinal order (the pre-window contract); skipped tuples tally at their
     skip point, which is itself ordered behind the stream head. */
  tuple->completed = 1;
  tuple->encoded_data = tuple->xor_accumulator;
  tuple->xor_accumulator = NULL;
  _clear_fetch_list(tuple->pending_fetches);
  tuple->pending_fetches = NULL;
  _clear_presatisfied_list(tuple->presatisfied);
  tuple->presatisfied = NULL;
  stream->state = OFF_STREAM_FETCHING_BLOCKS;
  _inflight_render_advance(stream);
}

/* Per-tuple fetch start: fold parked results in, get the rest. Mirrors the
 * single-tuple version's prune/comment contract, scoped to one tuple. */
static void _inflight_start_block_fetches(readable_off_stream_t* stream,
                                          inflight_tuple_t* tuple) {
  size_t count = tuple_size(tuple->tuple);
  tuple->blocks_expected = count;
  size_t presatisfied_count = 0;
  for (size_t i = 0; i < count; i++) {
    buffer_t* hash = tuple_get(tuple->tuple, i);
    pending_block_fetch_t* raced = NULL;
    pending_block_fetch_t** parked = &tuple->presatisfied;
    while (*parked != NULL) {
      pending_block_fetch_t* node = *parked;
      if (buffer_compare(node->hash, hash) == 0) {
        *parked = node->next;
        raced = node;
        break;
      }
      parked = &node->next;
    }
    if (raced != NULL) {
      DESTROY(raced->hash, buffer);
      if (raced->carried_block != NULL) {
        if (tuple->xor_accumulator == NULL) {
          tuple->xor_accumulator = buffer_copy(raced->carried_block->data);
        } else {
          buffer_t* xored = buffer_xor(tuple->xor_accumulator, raced->carried_block->data);
          DESTROY(tuple->xor_accumulator, buffer);
          tuple->xor_accumulator = xored;
        }
        DESTROY(raced->carried_block, block);
      }
      free(raced);
      presatisfied_count++;
    } else {
      pending_block_fetch_t* fetch = get_clear_memory(sizeof(pending_block_fetch_t));
      fetch->hash = (buffer_t*)refcounter_reference((refcounter_t*)hash);
      fetch->index = i;
      fetch->next = tuple->pending_fetches;
      tuple->pending_fetches = fetch;
      block_cache_get(stream->bc, hash, &stream->stream.actor);
    }
  }
  tuple->blocks_received = presatisfied_count;
  stream->state = OFF_STREAM_FETCHING_BLOCKS;
  if (tuple->blocks_received >= tuple->blocks_expected) {
    _inflight_complete(stream, tuple);
  }
}

/* Start a tuple: take a window slot, assign the ordinal, look the tuple up
 * in the tuple cache (or fetch straight away without one). */
static void _inflight_start(readable_off_stream_t* stream, tuple_t* tuple) {
  if (ATOMIC_LOAD(&stream->stream.is_deactivated)) {
    return;
  }
  inflight_tuple_t* entry = get_clear_memory(sizeof(inflight_tuple_t));
  entry->tuple = (tuple_t*)refcounter_reference((refcounter_t*)tuple);
  entry->ordinal = stream->next_ordinal++;
  /* FIFO append: the list stays ordinal-ordered, so the render drain can
     scan from the head. */
  entry->next = NULL;
  if (stream->inflight_tail == NULL) {
    stream->inflight_head = entry;
  } else {
    stream->inflight_tail->next = entry;
  }
  stream->inflight_tail = entry;
  stream->inflight_count++;
  if (stream->tc != NULL) {
    entry->in_lookup = 1;
    stream->state = OFF_STREAM_AWAITING_TUPLE;
    tuple_cache_get(stream->tc, tuple, &stream->stream.actor);
  } else {
    _inflight_start_block_fetches(stream, entry);
  }
}

/* Pop fully rendered entries from the head: renders (and the sent_bytes /
   close accounting in _render_origin_data) stay strictly in stream order —
   out-of-order decodes parked here until their turn. */
static void _inflight_render_advance(readable_off_stream_t* stream) {
  inflight_tuple_t* head;
  while ((head = stream->inflight_head) != NULL && head->completed) {
    if (ATOMIC_LOAD(&stream->stream.is_deactivated)) {
      return;
    }
    buffer_t* data = head->encoded_data;
    head->encoded_data = NULL;
    _tally_tuple_loaded(stream);
    if (data != NULL) {
      _render_origin_data(stream, data);
      DESTROY(data, buffer);
    }
    stream->inflight_head = head->next;
    if (stream->inflight_head == NULL) {
      stream->inflight_tail = NULL;
    }
    stream->inflight_count--;
    DESTROY(head->tuple, tuple);
    free(head);
  }
  /* A freed window slot may let a queued tuple start. */
  _drain_tuple_queue(stream);
}

/* Start tuples from the queue while the window has room. */
static void _drain_tuple_queue(readable_off_stream_t* stream) {
  if (ATOMIC_LOAD(&stream->stream.is_deactivated)) {
    return;
  }
  while (stream->tuple_queue != NULL &&
         stream->inflight_count < READ_STREAM_FETCH_WINDOW) {
    pending_tuple_t* head = stream->tuple_queue;
    stream->tuple_queue = head->next;
    tuple_t* tuple = head->tuple;
    free(head);
    _inflight_start(stream, tuple);
    DESTROY(tuple, tuple);
  }
}

/* Load-mode miss: drop the tuple, park its UNANSWERED hashes in the global
   stale list (late results for them are absorbed there), tally the skip,
   and free the window slot. In-order renders skip with it (no data). */
static void _inflight_skip(readable_off_stream_t* stream,
                           inflight_tuple_t* tuple) {
  if (tuple->xor_accumulator != NULL) {
    DESTROY(tuple->xor_accumulator, buffer);
    tuple->xor_accumulator = NULL;
  }
  _park_nodes_as_stale(&stream->stale_fetches, tuple->pending_fetches);
  tuple->pending_fetches = NULL;
  _clear_presatisfied_list(tuple->presatisfied);
  tuple->presatisfied = NULL;
  if (tuple->encoded_data != NULL) {
    DESTROY(tuple->encoded_data, buffer);
    tuple->encoded_data = NULL;
  }
  stream->tuples_skipped++;
  _notify_load_tuple(stream);
  /* Unlink (FIFO order preserved for the entries that remain). */
  inflight_tuple_t** current = &stream->inflight_head;
  inflight_tuple_t* prev_tail = NULL;
  while (*current != NULL) {
    if (*current == tuple) {
      *current = tuple->next;
      if (stream->inflight_tail == tuple) {
        prev_tail = tuple;
      }
      break;
    }
    prev_tail = *current == NULL ? prev_tail : prev_tail;
    current = &(*current)->next;
  }
  if (prev_tail != NULL) {
    stream->inflight_tail = NULL;
    inflight_tuple_t* walk = stream->inflight_head;
    while (walk != NULL) {
      stream->inflight_tail = walk;
      walk = walk->next;
    }
  }
  stream->inflight_count--;
  DESTROY(tuple->tuple, tuple);
  free(tuple);
  /* Removing the head may expose an already-completed entry that was
     stalled behind this tuple: advance the render drain. */
  _inflight_render_advance(stream);
}

void readable_off_stream_dispatch(void* state, message_t* msg) {
  readable_off_stream_t* stream = (readable_off_stream_t*)state;
  switch (msg->type) {
    case OFF_STREAM_WRITE: {
      tuple_t* tuple = (tuple_t*)msg->payload;
      if (ATOMIC_LOAD(&stream->stream.is_deactivated)) {
        DESTROY(tuple, tuple);
        msg->payload = NULL;
        break;
      }
      if (stream->inflight_count >= READ_STREAM_FETCH_WINDOW) {
        pending_tuple_t* queued = get_clear_memory(sizeof(pending_tuple_t));
        queued->tuple = (tuple_t*)refcounter_reference((refcounter_t*)tuple);
        queued->next = NULL;
        if (stream->tuple_queue == NULL) {
          stream->tuple_queue = queued;
        } else {
          pending_tuple_t* tail = stream->tuple_queue;
          while (tail->next != NULL) {
            tail = tail->next;
          }
          tail->next = queued;
        }
        DESTROY(tuple, tuple);
        msg->payload = NULL;
        break;
      }
      _inflight_start(stream, tuple);
      DESTROY(tuple, tuple);
      msg->payload = NULL;
      break;
    }
    case TUPLE_CACHE_GET_RESULT: {
      tuple_cache_get_result_payload_t* result =
          (tuple_cache_get_result_payload_t*)msg->payload;
      if (ATOMIC_LOAD(&stream->stream.is_deactivated)) {
        if (result->value != NULL) {
          DESTROY(result->value, buffer);
        }
        if (result->key != NULL) {
          DESTROY(result->key, tuple);
        }
        break;
      }
      /* Route to the in-flight lookup-stage tuple owning this key: compare
         the key tuple's first hash. */
      inflight_tuple_t* owner = NULL;
      if (result->key != NULL && tuple_size(result->key) > 0) {
        buffer_t* key_hash = tuple_get(result->key, 0);
        inflight_tuple_t* current = stream->inflight_head;
        while (current != NULL) {
          if (current->in_lookup &&
              _tuple_contains_hash(current->tuple, key_hash)) {
            owner = current;
            break;
          }
          current = current->next;
        }
      }
      if (owner == NULL) {
        /* Lookup-stage tuple destroyed meanwhile (skip/deactivate): drop. */
        if (result->value != NULL) {
          DESTROY(result->value, buffer);
        }
      } else if (result->value != NULL) {
        /* Tuple-cache hit: decoded data without any block fetch. */
        owner->in_lookup = 0;
        owner->completed = 1;
        owner->encoded_data = result->value;
        result->value = NULL;
        _inflight_render_advance(stream);
      } else {
        owner->in_lookup = 0;
        _inflight_start_block_fetches(stream, owner);
      }
      if (result->key != NULL) {
        DESTROY(result->key, tuple);
      }
      break;
    }
    case CACHE_GET_RESULT: {
      cache_get_result_payload_t* result = (cache_get_result_payload_t*)msg->payload;
      if (ATOMIC_LOAD(&stream->stream.is_deactivated)) {
        if (result->block != NULL) {
          DESTROY(result->block, block);
          result->block = NULL;
        }
        if (result->hash != NULL) {
          DESTROY(result->hash, buffer);
          result->hash = NULL;
        }
        break;
      }
      /* Late result for an already-skipped tuple: drop it. */
      if (_consume_stale_fetch(stream, result->hash)) {
        if (result->block != NULL) {
          DESTROY(result->block, block);
          result->block = NULL;
        }
        if (result->hash != NULL) {
          DESTROY(result->hash, buffer);
          result->hash = NULL;
        }
        break;
      }
      if (result->block == NULL) {
        /* Miss — the same semantics as the single-tuple version: the fetch
           node for this hash (whichever tuple issued it) stays outstanding
           and the network find will answer it. Issue the find. */
        if (stream->network != NULL) {
          stream->state = OFF_STREAM_AWAITING_NETWORK;
          network_local_find_block_payload_t* payload =
              get_clear_memory(sizeof(network_local_find_block_payload_t));
          payload->hash = REFERENCE(result->hash, buffer_t);
          payload->reply_to = &stream->stream.actor;
          payload->owner = (stream_t*)stream;
          refcounter_reference((refcounter_t*)stream);
          message_t net_msg;
          net_msg.type = NETWORK_LOCAL_FIND_BLOCK;
          net_msg.payload = payload;
          net_msg.payload_destroy = network_local_find_block_payload_destroy;
          actor_send(&stream->network->actor, &net_msg);
        } else if (stream->load_mode) {
          /* Local-only load: the fetch's owning tuple is the one whose node
             matches; it cannot be answered — skip it. */
          inflight_tuple_t* owner = _inflight_prune_fetch(stream, result->hash);
          if (owner != NULL) {
            _inflight_skip(stream, owner);
          }
        } else {
          /* Local-only get: any tuple missing a block cannot complete. */
          stream_deactivate((stream_t*)stream, OFFS_ERROR("Block not found in cache"));
        }
        if (result->hash != NULL) {
          DESTROY(result->hash, buffer);
          result->hash = NULL;
        }
        break;
      }

      /* Hit: prune the owner's fetch node; raced results (no node yet) park
       * on the tuple containing the hash; strays are dropped. Prune-first is
       * the exact single-tuple contract: only ONE copy of an answer may XOR
       * and count per tuple. */
      {
        uint8_t folded = 0;
        inflight_tuple_t* owner = _inflight_prune_fetch(stream, result->hash);
        if (owner == NULL) {
          owner = _inflight_contains_hash(stream, result->hash);
          if (owner != NULL) {
            _inflight_park_presatisfied(stream, owner, result->hash, result->block);
            result->block = NULL;
          } else if (result->hash == NULL) {
            /* Degenerate result without a hash: the pre-window contract
               accumulated it into the (sole) live tuple. Route it to the
               oldest in-flight tuple so the data is never dropped. */
            owner = stream->inflight_head;
            folded = 1;
          } else {
            result->block = NULL;
          }
        } else {
          folded = 1;
        }
        if (owner != NULL && folded) {
          if (owner->xor_accumulator == NULL) {
            owner->xor_accumulator = buffer_copy(result->block->data);
          } else {
            buffer_t* xored = buffer_xor(owner->xor_accumulator, result->block->data);
            DESTROY(owner->xor_accumulator, buffer);
            owner->xor_accumulator = xored;
          }
          owner->blocks_received++;
          result->block = NULL;
          if (owner->blocks_received >= owner->blocks_expected) {
            _inflight_complete(stream, owner);
          }
        }
        if (owner == NULL) {
          result->block = NULL;
        }
      }
      if (result->block != NULL) {
        DESTROY(result->block, block);
        result->block = NULL;
      }
      if (result->hash != NULL) {
        DESTROY(result->hash, buffer);
        result->hash = NULL;
      }
      break;
    }
    case NETWORK_FIND_BLOCK_RESULT: {
      network_find_block_result_payload_t* result =
          (network_find_block_result_payload_t*)msg->payload;
      if (_consume_stale_fetch(stream, result->hash)) {
        if (result->block != NULL) {
          DESTROY(result->block, block);
          result->block = NULL;
        }
        if (result->hash != NULL) {
          DESTROY(result->hash, buffer);
          result->hash = NULL;
        }
        break;
      }
      if (result->found) {
        if (result->block != NULL) {
          /* Direct-return: XOR into the owner tuple. Prune-first (one copy
             per tuple counts); raced answers park on the containing tuple. */
          uint8_t folded = 0;
          inflight_tuple_t* owner = _inflight_prune_fetch(stream, result->hash);
          if (owner == NULL) {
            owner = _inflight_contains_hash(stream, result->hash);
            if (owner != NULL) {
              _inflight_park_presatisfied(stream, owner, result->hash, result->block);
              result->block = NULL;
            } else if (result->hash == NULL) {
              /* Degenerate result without a hash: the pre-window contract
                 accumulated it into the (sole) live tuple. Route it to the
                 oldest in-flight tuple so the data is never dropped. */
              owner = stream->inflight_head;
              folded = 1;
            } else {
              result->block = NULL;
            }
          } else {
            folded = 1;
          }
          if (owner != NULL && folded) {
            if (owner->xor_accumulator == NULL) {
              owner->xor_accumulator = buffer_copy(result->block->data);
            } else {
              buffer_t* xored = buffer_xor(owner->xor_accumulator, result->block->data);
              DESTROY(owner->xor_accumulator, buffer);
              owner->xor_accumulator = xored;
            }
            owner->blocks_received++;
            result->block = NULL;
            if (owner->blocks_received >= owner->blocks_expected) {
              _inflight_complete(stream, owner);
            }
          }
          if (owner == NULL) {
            result->block = NULL;
          }
        } else {
          /* Found without bytes: re-fetch from the (now populated) cache for
             the owning tuple — same contract as the single-tuple version. */
          inflight_tuple_t* owner = result->hash != NULL
                                        ? _inflight_prune_fetch(stream, result->hash)
                                        : NULL;
          if (owner != NULL) {
            pending_block_fetch_t* fetch =
                get_clear_memory(sizeof(pending_block_fetch_t));
            fetch->hash = (buffer_t*)refcounter_reference((refcounter_t*)result->hash);
            fetch->index = 0;
            fetch->next = owner->pending_fetches;
            owner->pending_fetches = fetch;
            block_cache_get(stream->bc, result->hash, &stream->stream.actor);
            stream->state = OFF_STREAM_FETCHING_BLOCKS;
          }
        }
      } else if (stream->load_mode) {
        /* Not found anywhere: skip the requesting tuple (its node prunes
           here; unanswered siblings park as stale). */
        inflight_tuple_t* owner = _inflight_prune_fetch(stream, result->hash);
        if (owner != NULL) {
          _inflight_skip(stream, owner);
        }
      } else {
        /* Not found, get mode: the stream cannot complete. Deactivate; the
           error path tears the connection/pipeline down. */
        stream_deactivate((stream_t*)stream, OFFS_ERROR("Block not found on network"));
      }
      if (result->hash != NULL) {
        DESTROY(result->hash, buffer);
        result->hash = NULL;
      }
      break;
    }
    case CLOSE_STREAM: {
      if (!ATOMIC_LOAD(&stream->stream.is_deactivated)) {
        _close_stream(stream);
      }
      break;
    }
    default:
      stream_dispatch(state, msg);
      break;
  }
}

/* Run exactly the CLOSE_STREAM cleanup and notify close_event without an
 * error. Used by the CLOSE_STREAM message and by request_close so the two
 * entry points cannot drift. */
static void _close_stream(readable_off_stream_t* stream) {
  inflight_tuple_t* entry = stream->inflight_head;
  while (entry != NULL) {
    inflight_tuple_t* next = entry->next;
    if (entry->encoded_data != NULL) {
      DESTROY(entry->encoded_data, buffer);
    }
    if (entry->xor_accumulator != NULL) {
      DESTROY(entry->xor_accumulator, buffer);
    }
    _clear_fetch_list(entry->pending_fetches);
    _clear_presatisfied_list(entry->presatisfied);
    DESTROY(entry->tuple, tuple);
    free(entry);
    entry = next;
  }
  stream->inflight_head = NULL;
  stream->inflight_tail = NULL;
  stream->inflight_count = 0;
  pending_tuple_t* queued = stream->tuple_queue;
  while (queued != NULL) {
    pending_tuple_t* next = queued->next;
    DESTROY(queued->tuple, tuple);
    free(queued);
    queued = next;
  }
  stream->tuple_queue = NULL;
  _clear_stale_fetches(stream);
  stream->closed = 1;
  stream_notify((stream_t*)stream, close_event, NULL, NULL);
  ATOMIC_STORE(&stream->stream.is_deactivated, 1);
}

void readable_off_stream_request_close(readable_off_stream_t* stream) {
  if (stream == NULL) {
    return;
  }
  if (ATOMIC_LOAD(&stream->stream.is_deactivated) || stream->closed) {
    return;
  }
  _close_stream(stream);
}

static void _readable_off_stream_on_write(stream_t* stream, void* data) {
  (void)stream;
  (void)data;
}

readable_off_stream_t* readable_off_stream_create_ex(
    scheduler_pool_t* pool, block_cache_t* bc, tuple_cache_t* tc,
    ori_t* ori, size_t descriptor_pad, network_t* network, uint8_t load_mode) {
  readable_off_stream_t* stream = get_clear_memory(sizeof(readable_off_stream_t));
  stream->bc = bc;
  stream->tc = tc;
  stream->ori = ori;
  stream->network = network;
  stream->descriptor_pad = descriptor_pad;
  stream->offset_applied = 0;
  stream->load_mode = load_mode;
  stream->state = OFF_STREAM_FETCHING_BLOCKS;

  size_t block_size = _block_size_for_type(ori->block_type);
  stream->sent_bytes = ori->file_offset;
  stream->offset_remainder = ori->file_offset % block_size;

  stream_init((stream_t*)stream, push, transform_stream, 0, pool,
              (void (*)(stream_t*))readable_off_stream_destroy);
  stream->stream.on_write = _readable_off_stream_on_write;

  stream->stream.actor.state = stream;
  stream->stream.actor.dispatch = readable_off_stream_dispatch;

  return stream;
}

readable_off_stream_t* readable_off_stream_create(
    scheduler_pool_t* pool, block_cache_t* bc, tuple_cache_t* tc,
    ori_t* ori, size_t descriptor_pad, network_t* network) {
  return readable_off_stream_create_ex(pool, bc, tc, ori, descriptor_pad,
                                       network, 0);
}

void readable_off_stream_destroy(readable_off_stream_t* stream) {
  if (refcounter_dereference_is_zero((refcounter_t*)stream)) {
    pending_tuple_t* queued = stream->tuple_queue;
    while (queued != NULL) {
      pending_tuple_t* next = queued->next;
      DESTROY(queued->tuple, tuple);
      free(queued);
      queued = next;
    }
    inflight_tuple_t* entry = stream->inflight_head;
    while (entry != NULL) {
      inflight_tuple_t* next = entry->next;
      if (entry->encoded_data != NULL) {
        DESTROY(entry->encoded_data, buffer);
      }
      if (entry->xor_accumulator != NULL) {
        DESTROY(entry->xor_accumulator, buffer);
      }
      _clear_fetch_list(entry->pending_fetches);
      _clear_presatisfied_list(entry->presatisfied);
      DESTROY(entry->tuple, tuple);
      free(entry);
      entry = next;
    }
    _clear_stale_fetches(stream);
    /* Unregister this stream's wanted_list requesters BEFORE the struct (and
       its embedded actor) is freed, so a late NETWORK_FIND_BLOCK_RESULT can't
       actor_send into freed memory. See network_local_find_cancel_send. */
    network_local_find_cancel_send(stream->network, &stream->stream.actor);
    stream_deinit((stream_t*)stream);
    free(stream);
  }
}

void readable_off_stream_write(readable_off_stream_t* stream, tuple_t* tuple) {
  tuple_t* ref = REFERENCE(tuple, tuple_t);
  message_t msg;
  msg.type = OFF_STREAM_WRITE;
  msg.payload = ref;
  msg.payload_destroy = (void (*)(void*))tuple_destroy;

  actor_send(&stream->stream.actor, &msg);
}