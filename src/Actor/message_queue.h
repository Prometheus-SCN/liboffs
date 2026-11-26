//
// Created by victor on 5/6/25.
//

#ifndef OFFS_MESSAGE_QUEUE_H
#define OFFS_MESSAGE_QUEUE_H

#include "message.h"
#include "../Util/atomic_compat.h"
#include <stdbool.h>
#include <stddef.h>

#define MAILBOX_MUTE_THRESHOLD 256

typedef struct message_node_t message_node_t;
struct message_node_t {
  message_t msg;
  ATOMIC(message_node_t*) next;
  /* 0 = main FIFO, 1 = priority stack. The worker frees priority-sourced
     nodes directly after dispatch; main-queue nodes remain in the list as
     the pop sentinel (Michael-Scott tail trick) until a later pop frees
     them. */
  ATOMIC(uint8_t) source;
};

typedef struct message_queue_t {
  ATOMIC(message_node_t*) head;
  message_node_t* tail;
  /* Priority lane (consumer-critical messages jump the FIFO): a LIFO stack
     mutated only under send_lock; the worker pops it first (taking
     send_lock whenever it is non-empty). Single-consumer pop + locked
     producers make the stack race-free without ABA tags. */
  ATOMIC(message_node_t*) priority_head;
  ATOMIC(size_t) size;
  /* Optional back-pointer to the owning pool's pending-messages counter.
     When non-NULL, push increments and pop decrements it, so the pool always
     knows how many messages are queued across all of its actors. This is what
     scheduler_pool_wait_for_idle waits on (in addition to all workers being
     idle) so it never returns with an undelivered message still stranded in a
     mailbox. NULL for queues not owned by a scheduler pool. */
  ATOMIC(size_t)* pending_counter;
  /* Send-side spinlock + destroyed flag: serialize actor_send vs
     message_queue_destroy so the DESTROY-check-then-push TOCTOU in
     actor_send cannot fire. The single worker that pops/markempty's is
     not affected (it runs after destroy has waited for the worker via
     actor_destroy's RUNNING spin). See docs/concurrency-pass.md F4.

     `send_lock` is an embedded spinlock (0 = unlocked, 1 = locked) rather
     than a heap-allocated platform_mutex_t: a late sender (timer pd-loop
     thread, MsQuic callback) may arrive AFTER message_queue_destroy has
     returned and the containing actor struct may already have been freed
     by the caller. With a heap mutex we would have to either (a) keep the
     mutex alive forever (a per-actor leak that breaks valgrind) or (b)
     destroy it in message_queue_destroy, which makes any late send that
     races past the destroyed check dereference a freed/NULL mutex. The
     spinlock is embedded in the queue struct, so its lifetime is the
     struct's lifetime — no separate allocation, no destruction, no leak,
     and a late send that takes it observes `destroyed` and frees its
     message without UB. It uses ATOMIC(uint8_t) (not _Atomic bool) for
     cross-platform C/C++ compatibility via the atomic_compat.h macros.

     `destroyed` is a plain bool: it is only read/written while holding
     send_lock, so the spinlock provides all the synchronization needed. */
  ATOMIC(uint8_t) send_lock;
  bool destroyed;
} message_queue_t;

void message_queue_init(message_queue_t* queue);
/* Returns true on success (message queued; *was_empty, if non-NULL, receives
   whether the queue was empty before this push), false if the queue was
   destroyed (the message node AND its payload are freed by push in that
   case — the caller must NOT free them). */
bool message_queue_push(message_queue_t* queue, message_node_t* first, message_node_t* last, bool* was_empty);

/* Push onto the priority lane: the worker dispatches it before any queued
 * main-FIFO message (only the in-progress message delays it). Same was_empty
 * contract as push: *was_empty reports the MAIN queue's emptiness after the
 * push so the caller injects an idle actor. size/pending accounting is
 * shared with the main queue (backpressure semantics identical). */
bool message_queue_push_priority(message_queue_t* queue, message_node_t* node, bool* was_empty);
message_node_t* message_queue_pop(message_queue_t* queue);
bool message_queue_markempty(message_queue_t* queue);
bool message_queue_isempty(message_queue_t* queue);
void message_queue_destroy(message_queue_t* queue);

#endif // OFFS_MESSAGE_QUEUE_H
