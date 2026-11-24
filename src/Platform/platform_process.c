#ifdef _WIN32
  #define WIN32_LEAN_AND_MEAN
  #include <windows.h>
#else
  #define _DEFAULT_SOURCE
  #include <unistd.h>
  #include <sys/resource.h>
#endif

#include "platform_process.h"
#include "../Util/log.h"
#include <errno.h>

#ifdef _WIN32
  int platform_getpid(void) {
    return (int)GetCurrentProcessId();
  }

  int platform_core_count(void) {
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    return (int)info.dwNumberOfProcessors;
  }
#else
  int platform_getpid(void) {
    return (int)getpid();
  }

  int platform_core_count(void) {
    long count = sysconf(_SC_NPROCESSORS_ONLN);
    return (int)(count > 0 ? count : 1);
  }
#endif

/* Raise RLIMIT_NOFILE's soft limit to the hard limit. Long-running daemons
   exhaust the default soft cap (often 1024) after hours of churn (scheduler
   pools, QUIC endpoints, per-peer sockets, section/WAL/snapshot/open-file
   streams), after which every platform_file_open fails with EMFILE and the
   block cache degrades into 'sections_write FAILED result=3' until restart.
   POSIX raises soft → hard in one call; Windows handles carry no numeric fd
   cap, so the request is satisfied trivially. */
int platform_raise_nofile_limit(void) {
#ifdef _WIN32
  (void)0;  /* no fd-style numeric cap for HANDLEs */
  return 0;
#else
  struct rlimit limits;
  if (getrlimit(RLIMIT_NOFILE, &limits) != 0) {
    log_warn("platform_raise_nofile_limit: getrlimit(RLIMIT_NOFILE) failed (errno %d)",
             errno);
    return -1;
  }
  if (limits.rlim_cur >= limits.rlim_max || limits.rlim_max == RLIM_INFINITY) {
    return 0;  /* nothing to raise */
  }
  limits.rlim_cur = limits.rlim_max;
  if (setrlimit(RLIMIT_NOFILE, &limits) != 0) {
    log_warn("platform_raise_nofile_limit: setrlimit(RLIMIT_NOFILE) failed (errno %d)",
             errno);
    return -1;
  }
  log_info("platform_raise_nofile_limit: raised soft RLIMIT_NOFILE to hard limit");
  return 0;
#endif
}
