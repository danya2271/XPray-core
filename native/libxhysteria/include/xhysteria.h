#ifndef XHYSTERIA_H
#define XHYSTERIA_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define XHYS_ABI_VERSION UINT32_C(1)

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
void xhys_engine_join(void *engine);

#ifdef __cplusplus
}
#endif

#endif
