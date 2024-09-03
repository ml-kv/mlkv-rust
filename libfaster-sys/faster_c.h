// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

// C interface for the C++ code


#ifndef FASTER_C_H_
#define FASTER_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

typedef struct faster_t faster_t;

// Thread-related operations
void faster_complete_pending(faster_t* faster_t, bool wait);
void faster_start_session(faster_t* faster_t);
void faster_stop_session(faster_t* faster_t);

// Operations
faster_t* faster_open(const uint64_t table_size, const uint64_t log_size, const char* storage);
uint8_t faster_upsert(faster_t* faster_t, const uint64_t key, uint8_t* value, uint64_t value_length);
uint8_t faster_rmw(faster_t* faster_t, const uint64_t key, uint8_t* incr, const uint64_t value_length);
uint8_t faster_read(faster_t* faster_t, const uint64_t key, uint8_t* output);
uint8_t faster_delete(faster_t* faster_t, const uint64_t key);
uint8_t mlkv_read(faster_t* faster_t, const uint64_t key, uint8_t* output, const uint64_t value_length);
uint8_t mlkv_upsert(faster_t* faster_t, const uint64_t key, uint8_t* value, uint64_t value_length);
uint8_t mlkv_lookahead(faster_t* faster_t, const uint64_t key, const uint64_t value_length);
faster_t* faster_recover(const uint64_t table_size, const uint64_t log_size, const char* storage, const char* checkpoint_token);
bool faster_checkpoint(faster_t* faster_t);
void faster_destroy(faster_t* faster_t);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  /* FASTER_C_H_ */

