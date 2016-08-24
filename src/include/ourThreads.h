#include <pthread.h>
#include<stdio.h>

//Size of Queue holding jobs
//#define jobQueueSize 100
#define jobQueueSize 4
//Size of temporary buffer, which is used as destination for memcopy operation in helper thread
#define BufferSize 128*1024

//************************************************************
//Block sizes for prefetching data from NVM

//#define BlockSize 8192 //8KB
//#define BlockSize 12288 //12 KB
#define BlockSize 16384 //16KB Until now, 16KB block size gives best results in term of execution time
//#define BlockSize 20480//20 KB
//#define BlockSize 24576//24 KB
//#define BlockSize 32768 //32KB
//#define BlockSize 65536 //64KB

//*************************************************************
/*Struct which is used to pass arguments to the prefetchData function
 * startAddr: Addres to fetch data from BlkSize: This item is not
 * named in the proper way. Actually it is used to pass the remaing
 * (unfetched) size of file to prefetch_Data()
*/

typedef struct prefetch_args {
  char *srcAddr;
  off_t BlkSize;
}prefetch_args;


/* Job:
 * A struct which is used to represent the job to be perfomed by the prefetch_Data()
 * Each elemnt of job Queue holds an item of this type "job"
 * arg: a pointer variable used to hold the arguments to the prefetch_Data()
 * argPlaced: used to check if "arg" is place in job Queue. 1: valid value in queue element, 0: invalid value.
*/
typedef struct jobUnit{
    void*  arg;                          /* function's argument       */
    unsigned int argPlaced;
} jobUnit;

//**************************************************************
//a global thread ID used to create our ONLY thread
pthread_t gloablThrdeadID;
/*
 * fetch_mutex along with fetch_cv is used for controling r/w access
 * to job Queue which is accessed by both Postgres and helper thread.
*/
//pthread_mutex_t fetch_mutex;

/*
 * push_Index: used by postgres to insert jobs in the job queue.
 * pull_Index: used by helper thread for extracting jobs from job queue.
 * remJobs: indicates the jobs waiting queue to be serviced by helper thread at any given time
 */
int push_Index,pull_Index;
int remJobs,localRemJobs,loopIndex;


/*
 * prevFetchStartAdr: shows the address from where helper threads start prefetching.
 * prevFetchEndAdr: it is not used yet.
 */
char *prevFetchStartAdr;
char *prevFetchEndAdr;
/* Following variables are accessed only by helper thread.
 * tempBuffer: is the temporary buffer for prefetching data
 * remFileSize: indicates the remaining file size (which is not yet prefetched)
 * amount: amount of data to be copied in memcopy operation of helper thread
 */
char tempBuffer[BufferSize];
off_t remFileSize;
size_t amount;
/*
 * jobArray: holds job.
 * job: global variable used in fd.c
 * arg: gloabl variable used in fd.c
 */

jobUnit jobArray[jobQueueSize];
jobUnit job;
prefetch_args arg;

//******************************************************************
//initializ thread: This is called only once in fd.c by FileRead()
void initThread(pthread_t *thrdIdPtr,unsigned char cpuNo);
//thread routine which keeps waiting until there are jobs to be serviced in the queue
void waitLoop(void);
//A dummy function
void task1_our(void);
//prefetchData function (called by waitLoop function whene there are jobs in the job queue)
//It starts fetching data block from memory into the cache starting from a given address
void prefetch_Data(void *argRcvd);


