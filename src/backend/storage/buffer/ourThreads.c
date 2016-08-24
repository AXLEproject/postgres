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
            jobArray[pull_Index].arg=NULL;
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
    //******************************************************************************
    //mmemcopy based data prefetching
    //printf("prefetching data \n");
    //Multi-Block Prefetch
    //Note: if BlockSize=8192, following code will act as single-block fetcher.
    //This code can be improved since it always RE-fetch the overlapping blocks
    //(which ahve already been read in previous jobs)
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
        prevFetchEndAdr=prevFetchStartAdr+amount-1;
        memcpy(tempBuffer,prevFetchStartAdr,amount);
        }
}

void task1_our()
{

}
