#ifndef LOXDB_SD_STRESS_BENCH_ADMISSION_H
#define LOXDB_SD_STRESS_BENCH_ADMISSION_H

#include <stdint.h>

typedef struct bench_admission_profile_t bench_admission_profile_t;

struct bench_admission_profile_t {
  const char *name;
  uint16_t ram_kb;
  uint8_t kv_pct;
  uint8_t ts_pct;
  uint8_t rel_pct;
  uint8_t wal_compact_threshold_pct;
};

#endif /* LOXDB_SD_STRESS_BENCH_ADMISSION_H */

