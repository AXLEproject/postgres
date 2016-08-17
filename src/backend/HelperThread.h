#include <pthread.h>
//Naveed
struct HT_args {
  void *startAddr;
  int NumberOfBytes;
  int direction;
};
void* HelperThread(void *argRcvd);
//define your helper thread source code here
//startAddr: Addres to fetch data from
//NumberOfBytes: Number of bytes to be fetched, this argument must be multiple of 64 with smallest possible
//value of 64
//direction: 1= forward, -1=backword

//void* HelperThread(void *startAddr, int NumberOfBytes, int direction)
//void* HelperThread(struct HT_args *arg)

void* HelperThread(void *argRcvd)
{
    printf("\n thread is processing.\n");
    pthread_t thread = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);//clear cpuset
    pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    printf("thread id=%02x, cpuset=%d\n",thread,cpuset);


    struct HT_args *arg = (struct HT_args*)argRcvd;

    int LineSize=64;
    int loopcount=(arg->NumberOfBytes)/LineSize;
    //int loopcount= (*arg->NumberOfBytes)/LineSize;
    //int loopcount=(arg.NumberOfBytes)/LineSize;
    int i, ret1;
    for(i=0;i<loopcount;++i)
    {
        //__builtin_prefetch((arg.startAddr) + ((arg.direction)*i*LineSize) );
        __builtin_prefetch((arg->startAddr) + ((arg->direction)*i*LineSize) );
    }
    pthread_exit(&ret1);
    return NULL;
}


