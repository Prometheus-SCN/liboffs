#ifndef OFFS_PLATFORM_PROCESS_H
#define OFFS_PLATFORM_PROCESS_H

#ifdef __cplusplus
extern "C" {
#endif

int platform_getpid(void);
int platform_core_count(void);

/* Raise RLIMIT_NOFILE's soft limit to the hard limit (process-wide fd
   headroom). The scheduler + QUIC + block caches hold many descriptors for
   hours; the default soft cap (often 1024) makes every later
   platform_file_open fail once exhausted ('sections_write FAILED result=3'
   family — platform_file_open returning NULL on EMFILE). No-op on Windows
   (a HANDLE has no fd-style numeric cap). Returns 0 on success or after the
   no-op, -1 when the raise failed (callers log and continue). */
int platform_raise_nofile_limit(void);

/* Fill *used and *cap with the process's current open-descriptor count and
   the effective RLIMIT_NOFILE cap. Returns 0 on success (POSIX); -1 when
   unavailable (Windows: HANDLEs have no numeric cap, always -1). */
int platform_nofile_usage(size_t* used, size_t* cap);

#ifdef __cplusplus
}
#endif

#endif /* OFFS_PLATFORM_PROCESS_H */
