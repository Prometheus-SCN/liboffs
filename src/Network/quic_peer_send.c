//
// Created by victor on 5/16/25.
//

#include "quic_peer_send.h"
#include "quic_listener.h"
#include "stream_framer.h"
#include "connection_manager.h"
#include "peer_connection.h"
#include "msquic_singleton.h"
#include "../Util/allocator.h"
#include "../Util/log.h"
#include "../Platform/platform.h"
#include "../Platform/platform_time.h"
#include <cbor.h>
#include <string.h>

#ifdef HAS_MSQUIC
#include <msquic.h>
#endif

/* SEND_COMPLETE callback context — frees the frame buffer and QUIC_BUFFER
 * after msquic completes. The QUIC_BUFFER must be heap-allocated because
 * msquic stores a pointer to it and reads from it asynchronously in the
 * worker thread. */
typedef struct {
  uint8_t* frame;
  QUIC_BUFFER buf;
} send_complete_context_t;

#ifdef HAS_MSQUIC

static QUIC_STATUS QUIC_API PLATFORM_UNUSED _quic_send_complete_callback(
    HQUIC stream, void* context, QUIC_STREAM_EVENT* event) {
  (void)stream;
  if (event->Type == QUIC_STREAM_EVENT_SEND_COMPLETE) {
    send_complete_context_t* send_ctx = (send_complete_context_t*)context;
    if (send_ctx != NULL) {
      free(send_ctx->frame);
      free(send_ctx);
    }
  }
  return QUIC_STATUS_SUCCESS;
}

#endif /* HAS_MSQUIC */

int quic_peer_send(network_t* network, peer_connection_t* peer, cbor_item_t* cbor_msg) {
  if (network == NULL || peer == NULL || cbor_msg == NULL) return -1;

#ifdef HAS_MSQUIC
  if (peer->quic_stream == NULL) {
    log_error("quic_peer_send: peer has no QUIC stream");
    return -1;
  }
  /* Serialize CBOR to bytes */
  unsigned char* cbor_data = NULL;
  size_t cbor_len = 0;
  size_t serialized = cbor_serialize_alloc(cbor_msg, &cbor_data, &cbor_len);
  if (cbor_data == NULL || serialized == 0) {
    log_error("quic_peer_send: CBOR serialization failed");
    return -1;
  }

  /* Frame with 4-byte length prefix */
  size_t frame_len = 0;
  uint8_t* frame = stream_frame_encode(cbor_data, cbor_len, &frame_len);
  free(cbor_data);

  if (frame == NULL) {
    log_error("quic_peer_send: stream_frame_encode failed");
    return -1;
  }

  /* Create the SEND_COMPLETE context to free the frame buffer later.
   * The QUIC_BUFFER is included in the context struct so it remains
   * heap-allocated until SEND_COMPLETE fires — msquic stores a pointer
   * to the QUIC_BUFFER and reads from it asynchronously. */
  send_complete_context_t* send_ctx = get_clear_memory(sizeof(send_complete_context_t));
  if (send_ctx == NULL) {
    free(frame);
    log_error("quic_peer_send: failed to allocate send context");
    return -1;
  }
  send_ctx->frame = frame;
  send_ctx->buf.Buffer = frame;
  send_ctx->buf.Length = (uint32_t)frame_len;

  const struct QUIC_API_TABLE* msquic = offs_msquic_open();
  if (msquic == NULL) {
    free(frame);
    free(send_ctx);
    log_error("quic_peer_send: msquic API table not available");
    return -1;
  }

  /* OFFS-254 hunt diagnostic (remove with the block): tie every peer send
     to its stream handle and a global counter, so strands (StreamSend ok,
     SEND_COMPLETE never firing) are attributable to the exact stream. */
  static ATOMIC(uint64_t) quic_peer_send_count = ATOMIC_VAR_INIT(0);
  {
    uint64_t send_id = atomic_fetch_add_explicit(&quic_peer_send_count, 1,
                                                 memory_order_relaxed);
    log_error("quic:send stream=%p len=%zu id=%llu", (void*)peer->quic_stream,
              (size_t)frame_len, (unsigned long long)send_id);
  }
  QUIC_STATUS status = msquic->StreamSend(
      (HQUIC)peer->quic_stream,
      &send_ctx->buf,
      1,
      QUIC_SEND_FLAG_NONE,
      send_ctx);

  if (QUIC_FAILED(status)) {
    /* Release our msquic reference — the API table stays valid during the send */
    offs_msquic_close();
    free(frame);
    free(send_ctx);
    log_error("quic_peer_send: StreamSend failed: 0x%x", status);
    return -1;
  }

  /* Wedge watchdog (OFFS-269): this send rides with the stream context.
     When its SEND_COMPLETE fires the inflight count drops back; a send that
     never completes is visible to the network actor's 1s sweep via
     send_inflight/send_oldest_ms (on the context stored at StreamOpen),
     which then tears the peer down and reclaims the frame. */
  quic_stream_ctx_note_send(msquic->GetContext((HQUIC)peer->quic_stream));

  /* Release our msquic reference — the API table stays valid during the send */
  offs_msquic_close();

  return 0;

#else /* !HAS_MSQUIC */
  (void)network;
  log_error("quic_peer_send: no QUIC support (HAS_MSQUIC not defined)");
  return -1;
#endif
}

int quic_peer_send_path(network_t* network, node_id_t* path, uint8_t path_len, cbor_item_t* cbor_msg) {
  if (network == NULL || path == NULL || path_len == 0 || cbor_msg == NULL) return -1;

  /* The next hop is path[0] — the first node in the path */
  peer_connection_t* peer = connection_manager_lookup(&network->conn_mgr, &path[0]);
  if (peer == NULL) {
    log_error("quic_peer_send_path: no peer connection for next hop");
    return -1;
  }

  return quic_peer_send(network, peer, cbor_msg);
}

void quic_peer_broadcast(network_t* network, cbor_item_t* cbor_msg) {
  if (network == NULL || cbor_msg == NULL) return;

  for (size_t index = 0; index < network->conn_mgr.peer_count; index++) {
    peer_connection_t* peer = network->conn_mgr.peers[index];
    if (peer == NULL || !peer->connected) continue;

#ifdef HAS_MSQUIC
    if (peer->quic_stream == NULL) continue;
#endif

    /* Each call to quic_peer_send serializes the CBOR independently,
     * so we can pass the same cbor_msg pointer. The caller will
     * call cbor_decref once after broadcast completes. */
    quic_peer_send(network, peer, cbor_msg);
  }
}

/* Per-message stream send (OFFS-254 structural fix): each message rides its
 * own unidirectional stream — a wedged receive channel can no longer hoard
 * every subsequent frame for the peer, and msquic flow control bounds the
 * damage to one message. The persistent stream stays reserved for the
 * salutation/control exchange. Lifecycle: open uni stream on the peer's
 * connection, start, send, graceful shutdown (flush, then FIN); the accept
 * side reads one frame and the accept-side stream handler drives close via
 * the listener's deferred stack. */
#ifdef HAS_MSQUIC
#define QUIC_PEER_DATA_SEND_ATTEMPTS 4

static int quic_peer_send_data_attempt(network_t* network,
                                       peer_connection_t* peer,
                                       const struct QUIC_API_TABLE* msquic,
                                       quic_listener_t* listener,
                                       cbor_item_t* cbor_msg) {

  unsigned char* cbor_data = NULL;
  size_t cbor_len = 0;
  size_t serialized = cbor_serialize_alloc(cbor_msg, &cbor_data, &cbor_len);
  if (cbor_data == NULL || serialized == 0) {
    log_error("quic_peer_send_data: CBOR serialization failed");
    return -1;
  }

  size_t frame_len = 0;
  uint8_t* frame = stream_frame_encode(cbor_data, cbor_len, &frame_len);
  free(cbor_data);
  if (frame == NULL) {
    log_error("quic_peer_send_data: stream_frame_encode failed");
    return -1;
  }

  /* Stream context: same callback as accepted streams; this side only
     sends, so no framer (RECEIVE never indicates on an outbound uni
     stream). single_message=1 routes SHUTDOWN_COMPLETE to the deferred
     StreamClose. */
  quic_stream_context_t* stream_ctx = get_clear_memory(sizeof(quic_stream_context_t));
  if (stream_ctx == NULL) {
    free(frame);
    log_error("quic_peer_send_data: failed to allocate stream context");
    return -1;
  }
  stream_ctx->listener = listener;
  stream_ctx->connection = peer->quic_connection;
  uint32_t addr_len = sizeof(stream_ctx->peer_addr);
  msquic->GetParam((HQUIC)peer->quic_connection, QUIC_PARAM_CONN_REMOTE_ADDRESS,
                   &addr_len, &stream_ctx->peer_addr);
  stream_ctx->single_message = 1;

  HQUIC stream = NULL;
  QUIC_STATUS status = msquic->StreamOpen(
      (HQUIC)peer->quic_connection,
      QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL,
      quic_stream_callback,
      stream_ctx, &stream);
  if (QUIC_FAILED(status) || stream == NULL) {
    free(frame);
    free(stream_ctx);
    log_error("quic_peer_send_data: StreamOpen failed: 0x%x", status);
    return -1;
  }
  status = msquic->StreamStart(stream, QUIC_STREAM_START_FLAG_NONE);
  if (QUIC_FAILED(status)) {
    msquic->StreamShutdown((HQUIC)stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
    quic_listener_stream_close_deferred(listener, stream);
    free(frame);
    log_error("quic_peer_send_data: StreamStart failed: 0x%x", status);
    return -1;
  }

  send_complete_context_t* send_ctx = get_clear_memory(sizeof(send_complete_context_t));
  if (send_ctx == NULL) {
    msquic->StreamShutdown((HQUIC)stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
    quic_listener_stream_close_deferred(listener, stream);
    free(frame);
    log_error("quic_peer_send_data: failed to allocate send context");
    return -1;
  }
  send_ctx->frame = frame;
  send_ctx->buf.Buffer = frame;
  send_ctx->buf.Length = (uint32_t)frame_len;

  status = msquic->StreamSend((HQUIC)stream, &send_ctx->buf, 1,
                              QUIC_SEND_FLAG_NONE, send_ctx);
  if (QUIC_FAILED(status)) {
    msquic->StreamShutdown((HQUIC)stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
    quic_listener_stream_close_deferred(listener, stream);
    free(frame);
    free(send_ctx);
    log_error("quic_peer_send_data: StreamSend failed: 0x%x", status);
    return -1;
  }
  /* FIN after the queued data flushes — the receiver sees one complete
     frame and an end-of-stream. SEND_COMPLETE frees send_ctx/frame. */
  msquic->StreamShutdown((HQUIC)stream, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0);

  return 0;
}

#endif

int quic_peer_send_data(network_t* network, peer_connection_t* peer, cbor_item_t* cbor_msg) {
  if (network == NULL || peer == NULL || cbor_msg == NULL) return -1;

#ifdef HAS_MSQUIC
  if (peer->quic_connection == NULL) {
    log_error("quic_peer_send_data: peer has no QUIC connection");
    return -1;
  }
  quic_listener_t* listener = network->quic_listener;
  if (listener == NULL) {
    log_error("quic_peer_send_data: no QUIC listener");
    return -1;
  }
  const struct QUIC_API_TABLE* msquic = offs_msquic_open();
  if (msquic == NULL) {
    log_error("quic_peer_send_data: msquic API table not available");
    return -1;
  }

  /* A message can race the peer connection's teardown (wedge-abort →
     reconnect, mesh churn): StreamOpen fails INVALID_STATE even though the
     replacement connection is already live. Retry quietly — a dropped
     per-message send costs the full 30s wanted-list timeout on the
     consumer side. */
  for (int attempt = 0; attempt < QUIC_PEER_DATA_SEND_ATTEMPTS; attempt++) {
    if (attempt > 0) {
      if (peer->quic_connection == NULL) break;
      platform_sleep_ms(5u * (unsigned int)attempt);
      if (peer->quic_connection == NULL) break;
    }
    if (quic_peer_send_data_attempt(network, peer, msquic, listener, cbor_msg) == 0) {
      offs_msquic_close();
      return 0;
    }
  }

  offs_msquic_close();
  return -1;

#else /* !HAS_MSQUIC */
  (void)network;
  (void)peer;
  (void)cbor_msg;
  log_error("quic_peer_send_data: no QUIC support (HAS_MSQUIC not defined)");
  return -1;
#endif
}
