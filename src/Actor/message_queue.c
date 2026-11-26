//
// Created by victor on 5/6/25.
//

#include "message_queue.h"
#include "../Util/allocator.h"
#include "../Platform/platform_time.h"
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

static inline message_node_t* _set_empty_flag(message_node_t* ptr) {
  return (message_node_t*)((uintptr_t)ptr | 1);
}

static inline message_node_t* _clear_empty_flag(message_node_t* ptr) {
  return (message_node_t*)((uintptr_t)ptr & ~(uintptr_t)1);
}

static inline bool _is_empty_flag_set(message_node_t* ptr) {
  return ((uintptr_t)ptr & 1) != 0;
}

/* Embedded spinlock: 0 = unlocked, 1 = locked. Acquired by both the send
   path (actor_send from any thread) and message_queue_destroy, so the two
   cannot interleave. Not held by the worker's pop/markempty (the destroyer
   has already waited for RUNNING to clear). */
static inline void _send_lock_lock(message_queue_t* queue) {
  uint8_t expected = 0;
  while (!atomic_compare_exchange_weak_explicit(&queue->send_lock, &expected, 1,
                                                 memory_order_acquire, memory_order_relaxed)) {
    expected = 0;
    platform_sleep_ms(0);
  }
}

static inline void _send_lock_unlock(message_queue_t* queue) {
  atomic_store_explicit(&queue->send_lock, 0, memory_order_release);
}

void message_queue_init(message_queue_t* queue) {
  message_node_t* sentinel = get_clear_memory(sizeof(message_node_t));
  atomic_store(&sentinel->next, NULL);
  atomic_store(&sentinel->source, 0);
  atomic_store(&queue->head, _set_empty_flag(sentinel));
  queue->tail = sentinel;
  atomic_store(&queue->size, 0);
  queue->pending_counter = NULL;
  atomic_store(&queue->priority_head, NULL);
  atomic_store_explicit(&queue->send_lock, 0, memory_order_relaxed);
  queue->destroyed = false;
}

bool message_queue_push_priority(message_queue_t* queue, message_node_t* node,
                                 bool* was_empty) {
  if (was_empty != NULL) *was_empty = false;
  _send_lock_lock(queue);
  if (queue->destroyed) {
    _send_lock_unlock(queue);
    if (node->msg.payload_destroy != NULL && node->msg.payload != NULL) {
      node->msg.payload_destroy(node->msg.payload);
    }
    free(node);
    return false;
  }
  atomic_store(&node->source, 1);
  atomic_store(&node->next, NULL);
  message_node_t* prev_head = atomic_exchange(&queue->priority_head, node);
  atomic_store_explicit(&node->next, prev_head, memory_order_relaxed);
  atomic_fetch_add_explicit(&queue->size, 1, memory_order_relaxed);
  if (queue->pending_counter != NULL) {
    atomic_fetch_add_explicit(queue->pending_counter, 1, memory_order_release);
  }
  /* Main-queue-only emptiness: message_queue_isempty would see the priority
     node we just pushed and report non-empty, so an idle actor would never
     be injected (message stranded, pool wait_for_idle spins forever). The
     priority node itself rides the inject decision made for the MAIN queue
     (the worker's next pop checks the priority lane first anyway). */
  message_node_t* main_head = atomic_load_explicit(&queue->head, memory_order_relaxed);
  bool main_empty = _is_empty_flag_set(main_head) ||
                    _clear_empty_flag(main_head) == queue->tail;
  _send_lock_unlock(queue);
  if (was_empty != NULL) *was_empty = main_empty;
  return true;
}

bool message_queue_push(message_queue_t* queue, message_node_t* first, message_node_t* last, bool* was_empty) {
  if (was_empty != NULL) *was_empty = false;
  _send_lock_lock(queue);
  if (queue->destroyed) {
    _send_lock_unlock(queue);
    /* Free the node and its payload; the caller transferred ownership to
       push and has no way to recover it. */
    if (last->msg.payload_destroy != NULL && last->msg.payload != NULL) {
      last->msg.payload_destroy(last->msg.payload);
    }
    free(last);
    return false;
  }
  atomic_store_explicit(&last->next, NULL, memory_order_relaxed);
  atomic_thread_fence(memory_order_release);
  message_node_t* prev = atomic_exchange_explicit(&queue->head, last, memory_order_acq_rel);
  bool empty = _is_empty_flag_set(prev);
  prev = _clear_empty_flag(prev);
  atomic_store_explicit(&prev->next, first, memory_order_release);
  atomic_fetch_add_explicit(&queue->size, 1, memory_order_relaxed);
  if (queue->pending_counter != NULL) {
    atomic_fetch_add_explicit(queue->pending_counter, 1, memory_order_release);
  }
  _send_lock_unlock(queue);
  if (was_empty != NULL) *was_empty = empty;
  return true;
}

message_node_t* message_queue_pop(message_queue_t* queue) {
  /* Priority lane first. The stack is only mutated under send_lock; the
     pop takes the lock whenever it is non-empty (single worker here, so
     the fast empty-check is a plain load). */
  if (atomic_load_explicit(&queue->priority_head, memory_order_relaxed) != NULL) {
    _send_lock_lock(queue);
    message_node_t* node = queue->priority_head;
    if (node != NULL) {
      message_node_t* next = atomic_load_explicit(&node->next, memory_order_relaxed);
      atomic_store(&queue->priority_head, next);
      atomic_fetch_sub_explicit(&queue->size, 1, memory_order_relaxed);
      if (queue->pending_counter != NULL) {
        atomic_fetch_sub_explicit(queue->pending_counter, 1, memory_order_acq_rel);
      }
      _send_lock_unlock(queue);
      return node;  /* node ownership passes to the worker (freed post-dispatch) */
    }
    _send_lock_unlock(queue);
  }
  message_node_t* tail = queue->tail;
  message_node_t* next = atomic_load_explicit(&tail->next, memory_order_relaxed);
  if (next != NULL) {
    queue->tail = next;
    atomic_thread_fence(memory_order_acquire);
    atomic_fetch_sub_explicit(&queue->size, 1, memory_order_relaxed);
    if (queue->pending_counter != NULL) {
      atomic_fetch_sub_explicit(queue->pending_counter, 1, memory_order_acq_rel);
    }
    free(tail);
    return next;
  }
  return NULL;
}

bool message_queue_markempty(message_queue_t* queue) {
  message_node_t* tail = queue->tail;
  message_node_t* head = atomic_load_explicit(&queue->head, memory_order_relaxed);
  if (_is_empty_flag_set(head)) {
    return true;
  }
  if (head != tail) {
    return false;
  }
  return atomic_compare_exchange_strong_explicit(
    &queue->head, &head, _set_empty_flag(tail),
    memory_order_release, memory_order_relaxed);
}

bool message_queue_isempty(message_queue_t* queue) {
  if (atomic_load_explicit(&queue->priority_head, memory_order_relaxed) != NULL) {
    return false;
  }
  message_node_t* head = atomic_load_explicit(&queue->head, memory_order_relaxed);
  if (_is_empty_flag_set(head)) {
    return true;
  }
  return _clear_empty_flag(head) == queue->tail;
}

void message_queue_destroy(message_queue_t* queue) {
  /* Acquire the send_lock so any in-flight push either completes before we
     drain (and is freed by the pop loop below) or sees destroyed=true and
     frees itself. Set destroyed under the lock so the push path observes it
     consistently. The spinlock is not freed here — it is embedded in the
     queue struct, so a late sender can still safely take it after destroy
     returns and observe `destroyed` without use-after-free. */
  _send_lock_lock(queue);
  queue->destroyed = true;
  /* Drain the priority lane UNDER the already-held lock first:
     message_queue_pop takes send_lock whenever priority_head is non-NULL,
     and this thread already holds it — a non-recursive spinlock would
     self-deadlock. */
  {
    message_node_t* pnode = atomic_load(&queue->priority_head);
    while (pnode != NULL) {
      message_node_t* next = atomic_load(&pnode->next);
      if (pnode->msg.payload_destroy != NULL && pnode->msg.payload != NULL) {
        pnode->msg.payload_destroy(pnode->msg.payload);
      }
      free(pnode);
      pnode = next;
    }
    atomic_store(&queue->priority_head, NULL);
  }
  /* Drain the queue. message_queue_pop frees the previous sentinel and
     returns the next node (which becomes the new sentinel); the loop only
     destroys the payload of the returned node — the node itself is freed
     either by the next pop (as the prior sentinel) or by the final
     free(sentinel) below. */
  message_node_t* node;
  while ((node = message_queue_pop(queue)) != NULL) {
    if (node->msg.payload_destroy != NULL && node->msg.payload != NULL) {
      node->msg.payload_destroy(node->msg.payload);
    }
    if (atomic_load(&node->source) == 1) {
      free(node);
    }
  }
  message_node_t* sentinel = queue->tail;
  if (sentinel != NULL) {
    free(sentinel);
  }
  atomic_store(&queue->head, NULL);
  queue->tail = NULL;
  atomic_store(&queue->size, 0);
  _send_lock_unlock(queue);
}