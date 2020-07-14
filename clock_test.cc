#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <cinttypes>
#include <inttypes.h>

int main() {
    uint64_t clock1, clock2, time1, time2;
    unsigned hi1, lo1, hi2, lo2;
    timespec tp1, tp2;

    __asm__ __volatile__ ("rdtsc" : "=a"(lo1), "=d"(hi1));
    clock_gettime(CLOCK_MONOTONIC, &tp1);
    sleep(3);
    __asm__ __volatile__ ("rdtsc" : "=a"(lo2), "=d"(hi2));
    clock_gettime(CLOCK_MONOTONIC, &tp2);

    clock1 = ( (uint64_t)lo1)|( ((uint64_t)hi1)<<32 );
    clock2 = ( (uint64_t)lo2)|( ((uint64_t)hi2)<<32 );
    time1 = tp1.tv_sec * 1000000000 + tp1.tv_nsec;
    time2 = tp2.tv_sec * 1000000000 + tp2.tv_nsec;

    double t = clock2-clock1;
    t /= time2-time1;

    printf("clock2-clock1=%lu\n",clock2-clock1);
    printf("time2-time1=%lu\n",time2-time1);
    printf("(clock2-clock1)/(time2-time1)=%f\n",t);

    return 0;
}
