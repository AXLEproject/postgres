#include "work.h"
/*
prefetchData which starts fetching "NumberOfBtes" data from memory into the cache starting from "startAddress" in "direction".
*/

void HelperThread(void *argRcvd)
{
    printf("\n %d thread is processing.\n", (int)pthread_self());
//    pthread_t thread = pthread_self();
//    cpu_set_t cpuset;
//    CPU_ZERO(&cpuset);//clear cpuset
//    pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
//    printf("thread id=%02x, cpuset=%d\n",thread,cpuset);


    struct HT_args *arg = (struct HT_args*)argRcvd;

    int LineSize=64;
    int loopcount=(arg->NumberOfBytes)/LineSize;
    //int loopcount= (*arg->NumberOfBytes)/LineSize;
    //int loopcount=(arg.NumberOfBytes)/LineSize;
    int i;
    for(i=0;i<loopcount;++i)
    {
        //__builtin_prefetch((arg.startAddr) + ((arg.direction)*i*LineSize) );
        __builtin_prefetch((arg->startAddr) + ((arg->direction)*i*LineSize) );
    }
     printf("\n %d thread completes prefetching.\n", (int)pthread_self());
//    pthread_exit(&ret1);
//    return NULL;
}
