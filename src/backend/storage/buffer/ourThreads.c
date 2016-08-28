#include "ourThreads.h"
#include <unistd.h>


void waitLoop()
{
    /* Read job from queue and execute it */
    while(1)
    {

         if(__sync_bool_compare_and_swap (&(jobArray[pull_Index].argPlaced),1,0))
        {
            prefetch_Data(jobArray[pull_Index].arg);
            pull_Index++;
            pull_Index = pull_Index % jobQueueSize;
        }
    }
}

void initThread(pthread_t *thrdIdPtr, unsigned char cpuNo)
{

    pthread_attr_t attr;
    cpu_set_t cpus;

    pthread_attr_init(&attr);

    //uncomment following line AND comment SNİPPET1 to execute postgres and helper thread on same cpu
    //pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t),&cpus);

    /*
     * SNIPPET1:
     * Following two lines place the helper thread on cpu 3
     */

    CPU_ZERO(&cpus);
    //CPU_SET(3, &cpus);
    CPU_SET(cpuNo, &cpus);

    //set affinity attribute
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
    //create thread and launch for execution. The thread will invoke waitloop routine
    pthread_create(thrdIdPtr, &attr , (void *)waitLoop, NULL);
    pthread_detach(*thrdIdPtr);

}

void prefetch_Data(void *argRcvd)
{
    prefetch_args *arg = (prefetch_args*)argRcvd;
    if((arg->fetchType)==1)
    {
        //printf("helper thread: pull_index=%d\n",pull_Index);
        //******************************************************************************
        //mmemcopy based data prefetching
        //printf("prefetching data \n");
        //Multi-Block Prefetch
        //Note: if BlockSize=8192, following code will act as single-block fetcher.
        //This code can be improved since it always RE-fetch the overlapping blocks
        //(which ahve already been read in previous jobs)
        amount=arg->remainingFileSize;
        /*
        A check to make sure that always a valid value of remFileSize is received,
        Since in some queries this value turns out to be negative.
        */
        //if(remFileSize >= BlockSize)
        if(amount >0)
            {
            prevFetchStartAdr=arg->srcAddr;
            //prevFetchEndAdr=prevFetchStartAdr+amount-1;
            memcpy(tempBuffer,prevFetchStartAdr,amount);
            }
    }
    else if((arg->fetchType)==2)
    {
        //printf("helper thread: pull_index=%d\n",pull_Index);
        prevFetchStartAdr=arg->srcAddr;
        //__builtin_prefetch(prevFetchStartAdr - 64);
        //__builtin_prefetch(prevFetchStartAdr - 128);
        //__builtin_prefetch(prevFetchStartAdr - 192);
        //__builtin_prefetch(prevFetchStartAdr - 256);
        //__builtin_prefetch(prevFetchStartAdr - 320);
        //__builtin_prefetch(prevFetchStartAdr - 384);
        __builtin_prefetch(prevFetchStartAdr - 448);
        __builtin_prefetch(prevFetchStartAdr - 512);
        __builtin_prefetch(prevFetchStartAdr - 576);
        __builtin_prefetch(prevFetchStartAdr - 640);
        __builtin_prefetch(prevFetchStartAdr - 704);
        __builtin_prefetch(prevFetchStartAdr - 768);

    }

}

void task1_our()
{

}
