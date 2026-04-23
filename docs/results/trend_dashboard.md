# Results Trend Dashboard

- Generated UTC: 2026-04-19T17:32:44Z
- Source directory: C:\Users\vande\Desktop\github\loxdb\docs\results
- Window size: latest 20 runs/files

## Soak Trend
| Profile | Samples | SLO Pass Rate | Max KV Put (us) | Max TS Insert (us) | Max REL Insert (us) | Max Compact (us) | Max Reopen (us) | Max Spikes >5ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| balanced | 7 | 71.43% | 55840 | 70803 | 55174 | 33910 | 73484 | 6577 |
| deterministic | 7 | 100% | 34937 | 67679 | 77895 | 23004 | 76284 | 4341 |
| stress | 6 | 83.33% | 65093 | 71694 | 67176 | 37801 | 85819 | 8286 |

## Worstcase Matrix Trend
| Profile | Phase | Samples | SLO Pass Rate | Max KV Put (us) | Max TS Insert (us) | Max REL Insert (us) | Max TXN Commit (us) | Max Compact (us) | Max Reopen (us) | Max Spikes >5ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| balanced | aged | 10 | 100% | 37288 | 37943 | 44301 | 40835 | 34467 | 76041 | 276 |
| balanced | fresh | 10 | 100% | 19679 | 35195 | 33546 | 48991 | 29664 | 71677 | 30 |
| deterministic | aged | 5 | 100% | 34378 | 33623 | 33245 | 34704 | 27963 | 63712 | 210 |
| deterministic | fresh | 5 | 100% | 28314 | 32449 | 36121 | 45342 | 28058 | 60957 | 207 |
| stress | aged | 5 | 100% | 48603 | 46308 | 49320 | 47028 | 40384 | 85009 | 351 |
| stress | fresh | 5 | 100% | 26949 | 44909 | 46625 | 47658 | 42825 | 84043 | 348 |

## Latest Validation Summaries
- validation_summary_20260419_193234.md (updated UTC: 2026-04-19T17:32:44Z)
- validation_summary_20260419_180500.md (updated UTC: 2026-04-19T16:30:45Z)
- validation_summary_20260419_173440.md (updated UTC: 2026-04-19T16:01:30Z)
- validation_summary_20260419_152258.md (updated UTC: 2026-04-19T13:49:51Z)
- validation_summary_20260419_143541.md (updated UTC: 2026-04-19T13:02:35Z)
- validation_summary_20260412_214613.md (updated UTC: 2026-04-12T20:08:00Z)
- validation_summary_20260412_203442.md (updated UTC: 2026-04-12T18:34:49Z)
- validation_summary_20260412_200336.md (updated UTC: 2026-04-12T18:10:44Z)
- validation_summary_20260412_182103.md (updated UTC: 2026-04-12T16:28:10Z)
- validation_summary_20260412_181940.md (updated UTC: 2026-04-12T16:19:42Z)
- validation_summary_20260412_172006.md (updated UTC: 2026-04-12T16:19:03Z)
