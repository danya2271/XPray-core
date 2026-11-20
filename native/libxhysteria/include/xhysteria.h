#ifndef XHYSTERIA_H
#define XHYSTERIA_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define XHYS_ABI_VERSION UINT32_C(5)

typedef struct {
    uint8_t event_type;
    uint8_t flags;
    uint16_t reserved;
    uint64_t stream_id;
    uint32_t slab_idx;
    uint32_t data_len;
    uint32_t aux;
    uint32_t error_code;
} XhysEvent;

typedef struct {
    uint8_t *data;
    uint64_t len;
} XhysArenaView;

uint32_t xhys_abi_version(void);
void *xhys_bridge_create(uint64_t arena_bytes, uint32_t ring_capacity);
void xhys_bridge_destroy(void *bridge);
int32_t xhys_bridge_arena(void *bridge, XhysArenaView *view);
int32_t xhys_bridge_submit_command(void *bridge, const XhysEvent *event);
int32_t xhys_bridge_next_command(void *bridge, XhysEvent *event);
int32_t xhys_bridge_publish_event(void *bridge, const XhysEvent *event);
int32_t xhys_bridge_next_event(void *bridge, XhysEvent *event);
int32_t xhys_bridge_event_fd(void *bridge);
int32_t xhys_bridge_dup_event_fd(void *bridge);
void xhys_bridge_close(void *bridge);

/* Prototype socket bridge. Returned fd belongs to Go; udp_fd is duplicated.
 * Close the Go fd before joining. Config is copied before start returns. */
void *xhys_engine_start(const uint8_t *config_json, uintptr_t config_len,
                       int32_t udp_fd, int32_t *bridge_fd);
/* Aligned little-endian arena; all three returned fds belong to Go. Go must stop
 * all arena accesses before join, which releases the native allocation. */
typedef struct {
    uint8_t *data;
    uint64_t len;
    int32_t notify_fd;
    int32_t wake_fd;
    int32_t space_fd;
    void *owner;
} XhysSharedView;
void *xhys_engine_start_shared(const uint8_t *config_json, uintptr_t config_len,
                              int32_t udp_fd, XhysSharedView *view);
void xhys_engine_join(void *engine);
/* Live accepted QUIC connections, or -1 if untracked/stopped. Exclude join. */
int64_t xhys_engine_generation_connections(const void *engine);
int64_t xhys_engine_generation_work(const void *engine);
/* Experimental Linux packet ingress; not an activation/retirement API. */
void *xhys_ingress_start(const uint8_t *config_json, uintptr_t length, int32_t udp_fd);
bool xhys_ingress_add(void *ingress, uint8_t slot, int32_t worker_fd);
typedef struct {
    int32_t file;
    int32_t wake;
    int32_t space;
} XhysPacketRingDescriptors;
bool xhys_ingress_add_ring(void *ingress, uint8_t slot, int32_t worker_fd,
                          XhysPacketRingDescriptors *out);
bool xhys_ingress_activate(void *ingress, uint8_t slot);
uint64_t xhys_ingress_dropped(const void *ingress);
/* Approximate diagnostics, never use as proof that a generation is drained. */
typedef struct {
    uint64_t received;
    uint64_t forwarded;
    uint64_t backpressure;
    uint64_t queue_drops;
    uint64_t send_drops;
    uint64_t egress_drops;
    uint64_t queued_bytes;
    uint64_t peak_queued_bytes;
    uint64_t running;
} XhysIngressStats;
bool xhys_ingress_stats(const void *ingress, XhysIngressStats *stats);
void xhys_ingress_join(void *ingress);
void xhys_shared_release(void *owner);

#ifdef __cplusplus
}
#endif

#endif
