#include <pthread.h>
#include "thpool.h"
#define DIRECTION 1




/*
Struct to pass arguments to the prefetchData function
startAddr: Addres to fetch data from
NumberOfBytes: Number of bytes to be fetched, this argument must be multiple of 64 with smallest possible
value of 64
direction: 1= forward, -1=backword
*/
struct HT_args {
  char *srcAddr;  
  int NumberOfBytes;
  char direction;
};
/*
prefetchData which starts fetching "NumberOfBtes" data from memory into the cache starting from "startAddress" in "direction".
*/
void prefetchData(void *argRcvd);
//
threadpool thpoolGloabl1;

void task1();

void task2();

int ret1;
