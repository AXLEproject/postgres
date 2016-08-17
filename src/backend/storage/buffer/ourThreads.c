#include "ourThreads.h"
#include <unistd.h>
void waitLoop()
{
    /* Read job from queue and execute it */
    void*(*func_buff)(void* arg);
    void*  arg_buff;
    while(1)
    {

        //if(pull_Index>=(push_Index-1))
        {
        pthread_mutex_lock(&fetch_mutex);
        pthread_cond_wait(&fetch_cv, &fetch_mutex);
        pthread_mutex_unlock(&fetch_mutex);
        }

        //bsem_wait_our(&globalSem);

        //actual prefetching
        //printf("jobArray[%d].argPlaced=%d\n",jobQIndex,jobArray[jobQIndex].argPlaced);
        //if((jobArray[jobQIndex].argPlaced)==1)



        //printf("jobArray[%d].argPlaced=%d\n",(pull_Index+1)%jobQueueSize,jobArray[(pull_Index+1)%jobQueueSize].argPlaced);


        //while(pull_index<(push_Index-1))
        //while(pull_Index<push_Index)
        while(remJobs>0)
        {
            if((jobArray[(pull_Index+1)%jobQueueSize].argPlaced)==1)

            {
            //--indexDist;



            //printf("in the if block\n");




            jobUnit tempJob;

            //pthread_mutex_lock(&arrayLock);
            pull_Index=(++pull_Index)%jobQueueSize;
            --remJobs;
            //printf("helper thread: jobArray[%d].argPlaced=%d push_index=%d remJobs=%d \n",pull_Index,jobArray[pull_Index].argPlaced, push_Index,remJobs);

            /*
            if(pull_Index==jobQueueSize-1)
                ++pull_count;
            actPullIndex=pull_count*jobQueueSize+pull_Index;
            if (actPullIndex<(actPushIndex-Delta))
            {
                //printf("In Before: pull_count=%d push_count=%d actPullIndex=%d actPushIndex=%d\n",pull_count,push_count,actPullIndex,actPushIndex);
                //printf("In Before: pull_index=%d push_index=%d\n",pull_Index, push_Index);
                pull_Index=((actPushIndex-Delta)%jobQueueSize);
                //printf("In After: pull_index=%d push_index=%d\n",pull_Index, push_Index);
                pull_count=0;
                push_count=0;
            }
            */


            //printf("Out: pull_index=%d push_index=%d\n",pull_Index, push_Index);
            //printf("helper thread: jobQIndex=%d \n",jobQIndex);
            //printf("helper thread:%d pull_Index=%d \n",getpid(),pull_Index);
            //tempJob=jobArray[jobQIndex];
            tempJob=jobArray[pull_Index];
            //jobArray[jobQIndex].fetchedFlag=1;
            jobArray[pull_Index].argPlaced=0;
            //jobArray[jobQIndex].argPlaced=0;

            //pthread_mutex_unlock(&arrayLock);

            //func_buff=tempJob.function;
            //arg_buff=tempJob.arg;
            //func_buff(arg_buff);

            //start from here................
            //if((tempJob.fetchedFlag)!=1)
              //  {
                //printf("prefetching data for jobQIndex=%d\n",jobQIndex);
                prefetch_Data(tempJob.arg);
                //}
            }
        }
        //Empty Task

        //task1_our();


    }


}

void initThread(pthread_t *thrdIdPtr)
{
    //printf("In InıtThread function\n");

    pthread_attr_t attr;
    cpu_set_t cpus;

    //pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t),&cpus);

    pthread_attr_init(&attr);
    CPU_ZERO(&cpus);
    CPU_SET(3, &cpus);

    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);

    pthread_create(thrdIdPtr, &attr , (void *)waitLoop, NULL);
    //pthread_create(thrdIdPtr, NULL , (void *)waitLoop, NULL);
    pthread_detach(*thrdIdPtr);

}

void prefetch_Data(void *argRcvd)
{
    prefetch_args *arg = (prefetch_args*)argRcvd;
    //******************************************************************************
    /*
    //built_in prefetch  based data prefetching scheme
    char LineSize=64;
    int loopcount=(arg->NumberOfBytes)/LineSize;
    int i;
    char *basePtr=arg->srcAddr;
    for(i=0;i<loopcount;++i)
    {
        basePtr=basePtr+((arg->direction)*((unsigned char)i)*LineSize);
        __builtin_prefetch(basePtr);

    }
    */



    //******************************************************************************

    //printf("helper thread: pull_Index=%d\n",pull_Index);
    //printf("helper thread:%d pull_Index=%d prefetching data\n",getpid(),pull_Index);





    //printf("helper thread: prefetching data\n");
    char tempBuffer[BufferSize];
    tempBuffer[BufferSize-1]=0;


    //if((arg->direction==DIRECTION)&&((arg->srcAddr)!=NULL))
    if(arg->direction==DIRECTION)
    {
        //mmemcopy based data prefetching
        //printf("helper thread:%d srcAddr=%p direction=%d NumberOfBytes=%d\n",getpid(),arg->srcAddr,arg->direction,arg->NumberOfBytes);

        size_t amount=(size_t)(arg->NumberOfBytes);
        //char *srcAddr=(arg->srcAddr)+8192;
        char *srcAddr=(arg->srcAddr);

        if(amount>8192)
            amount=8192;

        //printf("helper thread:%d tempBuffer=%p srcAddr=%p amount=%d\n",getpid(),tempBuffer,srcAddr,amount);
        memcpy(tempBuffer,srcAddr, amount);

    }

}

void task1_our()
{

}
/* ======================== SYNCHRONISATION ========================= */
/*

// Init semaphore to 1 or 0
static void bsem_init_our(bsem_our *bsem_p, int value) {
    if (value < 0 || value > 1) {
        fprintf(stderr, "bsem_init(): Binary semaphore can take only values 1 or 0");
        exit(1);
    }
    pthread_mutex_init(&(bsem_p->mutex), NULL);
    pthread_cond_init(&(bsem_p->cond), NULL);
    bsem_p->v = value;
}


// Reset semaphore to 0
static void bsem_reset_our(bsem_our *bsem_p) {
    bsem_init(bsem_p, 0);
}


// Post to at least one thread
static void bsem_post_our(bsem_our*bsem_p) {
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p->v = 1;
    pthread_cond_signal(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);
}


//Post to all threads
static void bsem_post_all_our(bsem_our *bsem_p) {
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p->v = 1;
    pthread_cond_broadcast(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);
}


// Wait on semaphore until semaphore has value 0
static void bsem_wait_our(bsem_our * bsem_p) {
    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v != 1) {
        pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
    }
    bsem_p->v = 0;
    pthread_mutex_unlock(&bsem_p->mutex);
}

*/
