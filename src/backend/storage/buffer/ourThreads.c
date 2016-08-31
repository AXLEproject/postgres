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

void initThread(pthread_t *thrdIdPtr, int cpuNo)
{

    pthread_attr_t attr;
    cpu_set_t cpus;

    pthread_attr_init(&attr);

    //set affinity attribute
    CPU_ZERO(&cpus);    
    CPU_SET(cpuNo, &cpus);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);

    //create thread and launch for execution. The thread will invoke waitloop routine
    pthread_create(thrdIdPtr, &attr , (void *)waitLoop, NULL);

}

void prefetch_Data(void *argRcvd)
{
    prefetch_args *arg = (prefetch_args*)argRcvd;
    //******************************************************************************
    //mmemcopy based data prefetching    
    //Multi-Block Prefetch
    //Note: if BlockSize=8192, following code will act as single-block fetcher.    
    remFileSize=arg->BlkSize;
    /*
    A check to make sure that always a valid value of remFileSize is received,
    Since in some queries this value turns out to be negative.
    */
    if(remFileSize >= BlockSize)
        {
        amount=(size_t)remFileSize;
        //check to make sure that we do not prefetch more than specified BlockSize
        if(amount>BlockSize)
            {
            amount=BlockSize;
            }    

        prevFetchStartAdr=arg->srcAddr;        
        memcpy(tempBuffer,prevFetchStartAdr,amount);
        }
}

void task1_our()
{

}
