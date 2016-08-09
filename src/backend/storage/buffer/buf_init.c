/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "thpool.h"
#include "work.h"


BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
char	   **BufferBlocksPtr;
char	   **BufferBlocksPtr_save;


/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.  It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if an individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */


/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
void
InitBufferPool(void)
{
	bool		foundBufs;
	bool 		foundDescs;
    bool            foundBufsPtr;
    int             i;

    //Naveed
    //Extenstion
    //===============================================
    //initialize a thread pool with only one thread
    /*
    pid_t pid;
    if ((pid = getpid()) < 0) {
          perror("Buf_init.C unable to get pid");
        }
    else {
          printf("Buf_init.C The process id is %d", pid);
        }
    printf("Before thpoolGlobal1=%d\n",(int)thpoolGloabl1);
    printf("Making threadpool with 4 threads\n");
    thpoolGloabl1 = thpool_init(4);
    printf("After thpoolGlobal1=%d\n",(int)thpoolGloabl1);
    */
    /*
    for (i=0; i<20; i++){
        thpool_add_work(thpoolGloabl1, &task1, NULL);
        thpool_add_work(thpoolGloabl1, &task2, NULL);
        };
        */
    //===============================================

	/* Align descriptors to a cacheline boundary. */
	BufferDescriptors = (BufferDescPadded *) CACHELINEALIGN(
										ShmemInitStruct("Buffer Descriptors",
					NBuffers * sizeof(BufferDescPadded) + PG_CACHE_LINE_SIZE,
														&foundDescs));

	BufferBlocks = (char *)
		ShmemInitStruct("Buffer Blocks",
						NBuffers * (Size) BLCKSZ, &foundBufs);
        /* AAS: Allocate vector of pointers and its copy */
        BufferBlocksPtr = (char**) ShmemInitStruct("Buffer Blocks Ptr", NBuffers * sizeof(BufferBlocks), &foundBufsPtr);
        BufferBlocksPtr_save = (char**) ShmemInitStruct("Buffer Blocks Ptr Save", NBuffers * sizeof(BufferBlocks), &foundBufsPtr);

	if (foundDescs || foundBufs || foundBufsPtr)
	{
		/* all should be present or neither */
		Assert(foundDescs && foundBufs && foundBufsPtr);
		/* note: this path is only taken in EXEC_BACKEND case */
	}
	else
	{
                /* 
                 * AAS: Initialize the vector of pointers and save a copy 
                 */
                for(i =  0; i < NBuffers; i++) {
                    BufferBlocksPtr[i] = (char*)BufferBlocks + (i * BLCKSZ);
                    BufferBlocksPtr_save[i] = BufferBlocksPtr[i];
                }

		/*
		 * Initialize all the buffer headers.
		 */
		for (i = 0; i < NBuffers; i++)
		{
			BufferDesc *buf = GetBufferDescriptor(i);

			CLEAR_BUFFERTAG(buf->tag);
			buf->flags = 0;
			buf->usage_count = 0;
			buf->refcount = 0;
			buf->wait_backend_pid = 0;

			SpinLockInit(&buf->buf_hdr_lock);

			buf->buf_id = i;

			/*
			 * Initially link all the buffers together as unused. Subsequent
			 * management of this list is done by freelist.c.
			 */
			buf->freeNext = i + 1;

			buf->io_in_progress_lock = LWLockAssign();
			buf->content_lock = LWLockAssign();
		}

		/* Correct last entry of linked list */
		GetBufferDescriptor(NBuffers - 1)->freeNext = FREENEXT_END_OF_LIST;
	}

	/* Init other shared buffer-management stuff */
	StrategyInitialize(!foundDescs);
}

/*
 * BufferShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, hash tables, etc.
 */
Size
BufferShmemSize(void)
{
	Size		size = 0;

	/* size of buffer descriptors */
	size = add_size(size, mul_size(NBuffers, sizeof(BufferDescPadded)));
	size = add_size(size, mul_size(NBuffers*2, sizeof(BufferBlocks)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of data pages */
	size = add_size(size, mul_size(NBuffers, BLCKSZ));

	/* size of stuff controlled by freelist.c */
	size = add_size(size, StrategyShmemSize());

	return size;
}
