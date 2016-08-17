#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "work.h"
/*
prefetchData which starts fetching "NumberOfBtes" data from memory into the cache starting from "startAddress" in "direction".
*/
void task1(){
    printf("Thread #%u working on task1\n", (int)pthread_self());
}

void task2(){
    printf("Thread #%u working on task2\n", (int)pthread_self());
}

void prefetchData(void *argRcvd)
{    

    /*cpu_set_t cpus;

    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t),&cpus);
    printf("cpu mask of working thread = %d\n",cpus);
    */
/*
    pid_t pid;
    if ((pid = getpid()) < 0) {
          perror("PrefetchData unable to get pid\n");
        }
    else {
          printf("PrefetchData The process id is %d\n", pid);
        }
*/
    //printf("\n %d thread is processing.\n", (int)pthread_self());
//    pthread_t thread = pthread_self();
//    cpu_set_t cpuset;
//    CPU_ZERO(&cpuset);//clear cpuset
//    pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
//    printf("thread id=%02x, cpuset=%d\n",thread,cpuset);
    struct HT_args *arg = (struct HT_args*)argRcvd;
    if(arg->direction==DIRECTION)
    {
        printf("RCVR: PID=%d arg.srcAddr=%p  arg.destBuffer=%p arg.direction=%d arg.NumberOfBytes=%d\n",getpid(),arg->srcAddr,arg->destBuffer,arg->direction,arg->NumberOfBytes);

        /*
        //built_in prefetch  based data prefetching scheme
        char LineSize=64;
        int loopcount=(arg->NumberOfBytes)/LineSize;
        int i;
        char *basePtr=arg->startAddr;
        for(i=0;i<loopcount;++i)
        {
            basePtr=basePtr+((arg->direction)*((unsigned char)i)*LineSize);
            __builtin_prefetch(basePtr);

        }
        */


        //mmemcopy based data prefetching

        char *srcAddr=arg->srcAddr;
        char *dstBuffer=arg->destBuffer;
        size_t amount=(size_t)(arg->NumberOfBytes);
        if(amount>8192)
            amount=8192;
        //printf("dstPtr=%p SrcPtr=%p amount=%d\n",basePtr,tempBuffer,amount);
        memcpy(dstBuffer,srcAddr, amount);


        //Array based copy operation
        /*
        char *srcAddr=arg->srcAddr;
        char *dstBuffer=arg->destBuffer;
        size_t amount=(size_t)(arg->NumberOfBytes);
        if(amount>8192)
            amount=8192;
        int i;
        for(i=0;i<amount;++i)
        {
            dstBuffer[i]=srcAddr[i];
        }
        */
        //printf("\n %d thread prefetched %d bytes.\n", (int)pthread_self(),i*LineSize);

    }



//    pthread_exit(&ret1);
//    return NULL;
}
