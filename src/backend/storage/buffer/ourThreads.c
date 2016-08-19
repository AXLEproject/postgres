#include "ourThreads.h"
#include <unistd.h>


void waitLoop()
{
    /* Read job from queue and execute it */  
    while(1)
    {
        pthread_mutex_lock(&fetch_mutex);//lock mutex
        pthread_cond_wait(&fetch_cv, &fetch_mutex);//wait on cond var
        pthread_mutex_unlock(&fetch_mutex);//unlock mutex

        while(remJobs>0)//as long as there are jobs in queue, serve them
        {
            if((jobArray[(pull_Index+1)%jobQueueSize].argPlaced)==1)//if next queue elemnt has valid content
            {
            jobUnit tempJob;
            pull_Index=(++pull_Index)%jobQueueSize;//increment pull index
            --remJobs;//decrement number of waiting jobs
            tempJob=jobArray[pull_Index];//extract the job from queue
            //set the item zero to indicate that valid data has been
            //extracted and queue elemnet ah no more valid content
            jobArray[pull_Index].argPlaced=0;
            //call the prefetch_Data function
            prefetch_Data(tempJob.arg);
            }
        }
    }
}

void initThread(pthread_t *thrdIdPtr)
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
    CPU_SET(3, &cpus);

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
    //Multi-Block Prefetch
    //Note: if BlockSize=8192, following code will act as single-block fetcher.
    //This code can be improved since it always RE-fetch the overlapping blocks
    //(which ahve already been read in previous jobs)
    remFileSize=arg->BlkSize;
    /*
    A check to make sure that always a valid value of remFileSize is received,
    Since in some queries this value turns out to be negative.
    */
    if(remFileSize>0)
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
