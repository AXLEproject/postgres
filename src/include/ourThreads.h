#include <pthread.h>
#include<stdio.h>

#define jobQueueSize 100
#define DIRECTION 1
#define BufferSize 128*1024

//#define BufferSize 8193
//#define DELTA 3

pthread_t gloablThrdeadID;
//pthread_mutex_t arrayLock;
pthread_mutex_t fetch_mutex;
pthread_cond_t fetch_cv;
int push_Index,pull_Index;
int remJobs;
//int push_count,pull_count,Delta;
//int actPushIndex,actPullIndex;
//int jobQIndex;


/*
Struct to pass arguments to the prefetchData function
startAddr: Addres to fetch data from
NumberOfBytes: Number of bytes to be fetched, this argument must be multiple of 64 with smallest possible
value of 64
direction: 1= forward, -1=backword
*/

typedef struct prefetch_args {
  char *srcAddr;
  int NumberOfBytes;
  char direction;
}prefetch_args;


/* Job */
typedef struct jobUnit{
    //void*  (*function)(void* arg);       /* function pointer          */
    void*  arg;                          /* function's argument       */
    unsigned char argPlaced;
} jobUnit;


/* Binary semaphore */
/*
typedef struct bsem_our {
    pthread_mutex_t mutex;
    pthread_cond_t   cond;
    int v;
} bsem_our;

bsem_our globalSem;
*/
jobUnit jobArray[jobQueueSize];

void initThread(pthread_t *thrdIdPtr);
void waitLoop(void);
void task1_our(void);
//prefetchData which starts fetching "NumberOfBtes" data from memory into the cache starting from "startAddress" in "direction".
void prefetch_Data(void *argRcvd);

/*
static void  bsem_init_our(struct bsem_our *bsem_p, int value);
static void  bsem_reset_our(struct bsem_our *bsem_p);
static void  bsem_post_our(struct bsem_our *bsem_p);
static void  bsem_post_all_our(struct bsem_our *bsem_p);
static void  bsem_wait_our(struct bsem_our *bsem_p);
*/
