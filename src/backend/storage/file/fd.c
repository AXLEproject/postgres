/*-------------------------------------------------------------------------
 *
 * fd.c
 *	  Virtual file descriptor code.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/fd.c
 *
 * NOTES:
 *
 * This code manages a cache of 'virtual' file descriptors (VFDs).
 * The server opens many file descriptors for a variety of reasons,
 * including base tables, scratch files (e.g., sort and hash spool
 * files), and random calls to C library routines like system(3); it
 * is quite easy to exceed system limits on the number of open files a
 * single process can have.  (This is around 256 on many modern
 * operating systems, but can be as low as 32 on others.)
 *
 * VFDs are managed as an LRU pool, with actual OS file descriptors
 * being opened and closed as needed.  Obviously, if a routine is
 * opened using these interfaces, all subsequent operations must also
 * be through these interfaces (the File type is not a real file
 * descriptor).
 *
 * For this scheme to work, most (if not all) routines throughout the
 * server should use these interfaces instead of calling the C library
 * routines (e.g., open(2) and fopen(3)) themselves.  Otherwise, we
 * may find ourselves short of real file descriptors anyway.
 *
 * INTERFACE ROUTINES
 *
 * PathNameOpenFile and OpenTemporaryFile are used to open virtual files.
 * A File opened with OpenTemporaryFile is automatically deleted when the
 * File is closed, either explicitly or implicitly at end of transaction or
 * process exit. PathNameOpenFile is intended for files that are held open
 * for a long time, like relation files. It is the caller's responsibility
 * to close them, there is no automatic mechanism in fd.c for that.
 *
 * AllocateFile, AllocateDir, OpenPipeStream and OpenTransientFile are
 * wrappers around fopen(3), opendir(3), popen(3) and open(2), respectively.
 * They behave like the corresponding native functions, except that the handle
 * is registered with the current subtransaction, and will be automatically
 * closed at abort. These are intended mainly for short operations like
 * reading a configuration file; there is a limit on the number of files that
 * can be opened using these functions at any one time.
 *
 * Finally, BasicOpenFile is just a thin wrapper around open() that can
 * release file descriptors in use by the virtual file descriptors if
 * necessary. There is no automatic cleanup of file descriptors returned by
 * BasicOpenFile, it is solely the caller's responsibility to close the file
 * descriptor by calling close(2).
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/file.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>		/* for getrlimit */
#endif

#include "miscadmin.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/resowner_private.h"

#include <sys/mman.h>
//Naveed
//========================
//#include "HelperThread.h"
//#include "threadPool/thpool.h"
//#include "work.h"
#include "ourThreads.h"
//========================

/* Define PG_FLUSH_DATA_WORKS if we have an implementation for pg_flush_data */
#if defined(HAVE_SYNC_FILE_RANGE)
#define PG_FLUSH_DATA_WORKS 1
#elif defined(USE_POSIX_FADVISE) && defined(POSIX_FADV_DONTNEED)
#define PG_FLUSH_DATA_WORKS 1
#endif

/*
 * We must leave some file descriptors free for system(), the dynamic loader,
 * and other code that tries to open files without consulting fd.c.  This
 * is the number left free.  (While we can be pretty sure we won't get
 * EMFILE, there's never any guarantee that we won't get ENFILE due to
 * other processes chewing up FDs.  So it's a bad idea to try to open files
 * without consulting fd.c.  Nonetheless we cannot control all code.)
 *
 * Because this is just a fixed setting, we are effectively assuming that
 * no such code will leave FDs open over the long term; otherwise the slop
 * is likely to be insufficient.  Note in particular that we expect that
 * loading a shared library does not result in any permanent increase in
 * the number of open files.  (This appears to be true on most if not
 * all platforms as of Feb 2004.)
 */
#define NUM_RESERVED_FDS		10

/*
 * If we have fewer than this many usable FDs after allowing for the reserved
 * ones, choke.
 */
#define FD_MINFREE				10


/*
 * A number of platforms allow individual processes to open many more files
 * than they can really support when *many* processes do the same thing.
 * This GUC parameter lets the DBA limit max_safe_fds to something less than
 * what the postmaster's initial probe suggests will work.
 */
int			max_files_per_process = 1000;

/*
 * Maximum number of file descriptors to open for either VFD entries or
 * AllocateFile/AllocateDir/OpenTransientFile operations.  This is initialized
 * to a conservative value, and remains that way indefinitely in bootstrap or
 * standalone-backend cases.  In normal postmaster operation, the postmaster
 * calls set_max_safe_fds() late in initialization to update the value, and
 * that value is then inherited by forked subprocesses.
 *
 * Note: the value of max_files_per_process is taken into account while
 * setting this variable, and so need not be tested separately.
 */
int			max_safe_fds = 32;	/* default if not changed */


/* Debugging.... */

/*#define FDDEBUG 1*/
#ifdef FDDEBUG
#define DO_DB(A) \
	do { \
		int			_do_db_save_errno = errno; \
		A; \
		errno = _do_db_save_errno; \
	} while (0)
#else
#define DO_DB(A) \
	((void) 0)
#endif

#define VFD_CLOSED (-1)

#define FileIsValid(file) \
	((file) > 0 && (file) < (int) SizeVfdCache && VfdCache[file].fileName != NULL)

#define FileIsMapped(file) (VfdCache[file].pm_list_idx > -1)

#define FileIsNotOpen(file) (VfdCache[file].fd == VFD_CLOSED)

#define FileUnknownPos ((off_t) -1)

/* these are the assigned bits in fdstate below: */
#define FD_TEMPORARY		(1 << 0)	/* T = delete when closed */
#define FD_XACT_TEMPORARY	(1 << 1)	/* T = delete at eoXact */

typedef struct vfd
{
	int			fd;				/* current FD, or VFD_CLOSED if none */
        void*                   pm_ptr_list[128*1024]; /* AAS: Pointers to the mapped region for persistent memory, dirty */
        off_t                   pm_size_list[128*1024]; /* AAS: Sizes of the mapping, cumulative */
        int                     pm_list_idx;
	unsigned short fdstate;		/* bitflags for VFD's state */
	ResourceOwner resowner;		/* owner, for automatic cleanup */
	File		nextFree;		/* link to next free VFD, if in freelist */
	File		lruMoreRecently;	/* doubly linked recency-of-use list */
	File		lruLessRecently;
	off_t		seekPos;		/* current logical file position */
	off_t		fileSize;		/* current size of file (0 if not temporary) */
	char	   *fileName;		/* name of file, or NULL for unused VFD */
	/* NB: fileName is malloc'd, and must be free'd when closing the VFD */
	int			fileFlags;		/* open(2) flags for (re)opening the file */
	int			fileMode;		/* mode to pass to open(2) */
} Vfd;

/*
 * Virtual File Descriptor array pointer and size.  This grows as
 * needed.  'File' values are indexes into this array.
 * Note that VfdCache[0] is not a usable VFD, just a list header.
 */
static Vfd *VfdCache;
static Size SizeVfdCache = 0;

/*
 * Number of file descriptors known to be in use by VFD entries.
 */
static int	nfile = 0;

/*
 * Flag to tell whether it's worth scanning VfdCache looking for temp files
 * to close
 */
static bool have_xact_temporary_files = false;

/*
 * Tracks the total size of all temporary files.  Note: when temp_file_limit
 * is being enforced, this cannot overflow since the limit cannot be more
 * than INT_MAX kilobytes.  When not enforcing, it could theoretically
 * overflow, but we don't care.
 */
static uint64 temporary_files_size = 0;

/*
 * List of OS handles opened with AllocateFile, AllocateDir and
 * OpenTransientFile.
 */
typedef enum
{
	AllocateDescFile,
	AllocateDescPipe,
	AllocateDescDir,
	AllocateDescRawFD
} AllocateDescKind;

typedef struct
{
	AllocateDescKind kind;
	SubTransactionId create_subid;
	union
	{
		FILE	   *file;
		DIR		   *dir;
		int			fd;
	}			desc;
} AllocateDesc;

static int	numAllocatedDescs = 0;
static int	maxAllocatedDescs = 0;
static AllocateDesc *allocatedDescs = NULL;

/*
 * Number of temporary files opened during the current session;
 * this is used in generation of tempfile names.
 */
static long tempFileCounter = 0;

/*
 * Array of OIDs of temp tablespaces.  When numTempTableSpaces is -1,
 * this has not been set in the current transaction.
 */
static Oid *tempTableSpaces = NULL;
static int	numTempTableSpaces = -1;
static int	nextTempTableSpace = 0;


/*--------------------
 *
 * Private Routines
 *
 * Delete		   - delete a file from the Lru ring
 * LruDelete	   - remove a file from the Lru ring and close its FD
 * Insert		   - put a file at the front of the Lru ring
 * LruInsert	   - put a file at the front of the Lru ring and open it
 * ReleaseLruFile  - Release an fd by closing the last entry in the Lru ring
 * ReleaseLruFiles - Release fd(s) until we're under the max_safe_fds limit
 * AllocateVfd	   - grab a free (or new) file record (from VfdArray)
 * FreeVfd		   - free a file record
 *
 * The Least Recently Used ring is a doubly linked list that begins and
 * ends on element zero.  Element zero is special -- it doesn't represent
 * a file and its "fd" field always == VFD_CLOSED.  Element zero is just an
 * anchor that shows us the beginning/end of the ring.
 * Only VFD elements that are currently really open (have an FD assigned) are
 * in the Lru ring.  Elements that are "virtually" open can be recognized
 * by having a non-null fileName field.
 *
 * example:
 *
 *	   /--less----\				   /---------\
 *	   v		   \			  v			  \
 *	 #0 --more---> LeastRecentlyUsed --more-\ \
 *	  ^\									| |
 *	   \\less--> MostRecentlyUsedFile	<---/ |
 *		\more---/					 \--less--/
 *
 *--------------------
 */
static void Delete(File file);
static void LruDelete(File file);
static void Insert(File file);
static int	LruInsert(File file);
static bool ReleaseLruFile(void);
static void ReleaseLruFiles(void);
static File AllocateVfd(void);
static void FreeVfd(File file);

static int	FileAccess(File file);
static File OpenTemporaryFileInTablespace(Oid tblspcOid, bool rejectError);
static bool reserveAllocatedDesc(void);
static int	FreeDesc(AllocateDesc *desc);
static struct dirent *ReadDirExtended(DIR *dir, const char *dirname, int elevel);

static void AtProcExit_Files(int code, Datum arg);
static void CleanupTempFiles(bool isProcExit);
static void RemovePgTempFilesInDir(const char *tmpdirname);
static void RemovePgTempRelationFiles(const char *tsdirname);
static void RemovePgTempRelationFilesInDbspace(const char *dbspacedirname);
static bool looks_like_temp_rel_name(const char *name);

static void walkdir(const char *path,
		void (*action) (const char *fname, bool isdir, int elevel),
		bool process_symlinks,
		int elevel);
#ifdef PG_FLUSH_DATA_WORKS
static void pre_sync_fname(const char *fname, bool isdir, int elevel);
#endif
static void fsync_fname_ext(const char *fname, bool isdir, int elevel);
static int getIdxFromSeekpos(File file);


/*
 * AAS: pg_msync --- do msync for persistent memory backend
 */
int
pg_msync(File file)
{
    int i, returnCode;
	DO_DB(elog(LOG, "pg_msync: %d (%s)",
			   file, VfdCache[file].fileName));
    /* AAS: Call msync with appropiate parameters */
    Assert(FileIsMapped(file));
    if (enableFsync) {
        for (i = 0; i <= VfdCache[file].pm_list_idx; i++) {
	    DO_DB(elog(LOG, "pg_msync: %d (%s), ptr: %p, size %lu",
			   file, VfdCache[file].fileName, VfdCache[file].pm_ptr_list[i], VfdCache[file].pm_size_list[i]));
            returnCode =  msync(VfdCache[file].pm_ptr_list[i], VfdCache[file].pm_size_list[i], MS_SYNC);
            if (returnCode != 0) return returnCode;
        }
        return 0;
    } else {
        return 0;
    }
}

/*
 * pg_fsync --- do fsync with or without writethrough
 */
int
pg_fsync(int fd)
{
	/* #if is to skip the sync_method test if there's no need for it */
#if defined(HAVE_FSYNC_WRITETHROUGH) && !defined(FSYNC_WRITETHROUGH_IS_FSYNC)
	if (sync_method == SYNC_METHOD_FSYNC_WRITETHROUGH)
		return pg_fsync_writethrough(fd);
	else
#endif
		return pg_fsync_no_writethrough(fd);
}


/*
 * pg_fsync_no_writethrough --- same as fsync except does nothing if
 *	enableFsync is off
 */
int
pg_fsync_no_writethrough(int fd)
{
	if (enableFsync)
		return fsync(fd);
	else
		return 0;
}

/*
 * pg_fsync_writethrough
 */
int
pg_fsync_writethrough(int fd)
{
	if (enableFsync)
	{
#ifdef WIN32
		return _commit(fd);
#elif defined(F_FULLFSYNC)
		return (fcntl(fd, F_FULLFSYNC, 0) == -1) ? -1 : 0;
#else
		errno = ENOSYS;
		return -1;
#endif
	}
	else
		return 0;
}

/*
 * pg_fdatasync --- same as fdatasync except does nothing if enableFsync is off
 *
 * Not all platforms have fdatasync; treat as fsync if not available.
 */
int
pg_fdatasync(int fd)
{
	if (enableFsync)
	{
#ifdef HAVE_FDATASYNC
		return fdatasync(fd);
#else
		return fsync(fd);
#endif
	}
	else
		return 0;
}

/*
 * pg_flush_data --- advise OS that the data described won't be needed soon
 *
 * Not all platforms have sync_file_range or posix_fadvise; treat as no-op
 * if not available.  Also, treat as no-op if enableFsync is off; this is
 * because the call isn't free, and some platforms such as Linux will actually
 * block the requestor until the write is scheduled.
 */
int
pg_flush_data(int fd, off_t offset, off_t amount)
{
#ifdef PG_FLUSH_DATA_WORKS
	if (enableFsync)
	{
#if defined(HAVE_SYNC_FILE_RANGE)
		return sync_file_range(fd, offset, amount, SYNC_FILE_RANGE_WRITE);
#elif defined(USE_POSIX_FADVISE) && defined(POSIX_FADV_DONTNEED)
		return posix_fadvise(fd, offset, amount, POSIX_FADV_DONTNEED);
#else
#error PG_FLUSH_DATA_WORKS should not have been defined
#endif
	}
#endif
	return 0;
}


/*
 * fsync_fname -- fsync a file or directory, handling errors properly
 *
 * Try to fsync a file or directory. When doing the latter, ignore errors that
 * indicate the OS just doesn't allow/require fsyncing directories.
 */
void
fsync_fname(char *fname, bool isdir)
{
	int			fd;
	int			returncode;

	/*
	 * Some OSs require directories to be opened read-only whereas other
	 * systems don't allow us to fsync files opened read-only; so we need both
	 * cases here
	 */
	if (!isdir)
		fd = OpenTransientFile(fname,
							   O_RDWR | PG_BINARY,
							   S_IRUSR | S_IWUSR);
	else
		fd = OpenTransientFile(fname,
							   O_RDONLY | PG_BINARY,
							   S_IRUSR | S_IWUSR);

	/*
	 * Some OSs don't allow us to open directories at all (Windows returns
	 * EACCES)
	 */
	if (fd < 0 && isdir && (errno == EISDIR || errno == EACCES))
		return;

	else if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", fname)));

	returncode = pg_fsync(fd);

	/* Some OSs don't allow us to fsync directories at all */
	if (returncode != 0 && isdir && errno == EBADF)
	{
		CloseTransientFile(fd);
		return;
	}

	if (returncode != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", fname)));

	CloseTransientFile(fd);
}


/*
 * InitFileAccess --- initialize this module during backend startup
 *
 * This is called during either normal or standalone backend start.
 * It is *not* called in the postmaster.
 */
void
InitFileAccess(void)
{
	Assert(SizeVfdCache == 0);	/* call me only once */

	/* initialize cache header entry */
	VfdCache = (Vfd *) malloc(sizeof(Vfd));
	if (VfdCache == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	MemSet((char *) &(VfdCache[0]), 0, sizeof(Vfd));
	VfdCache->fd = VFD_CLOSED;
	VfdCache->pm_list_idx = -1;

	SizeVfdCache = 1;

	/* register proc-exit hook to ensure temp files are dropped at exit */
	on_proc_exit(AtProcExit_Files, 0);
}

/*
 * count_usable_fds --- count how many FDs the system will let us open,
 *		and estimate how many are already open.
 *
 * We stop counting if usable_fds reaches max_to_probe.  Note: a small
 * value of max_to_probe might result in an underestimate of already_open;
 * we must fill in any "gaps" in the set of used FDs before the calculation
 * of already_open will give the right answer.  In practice, max_to_probe
 * of a couple of dozen should be enough to ensure good results.
 *
 * We assume stdin (FD 0) is available for dup'ing
 */
static void
count_usable_fds(int max_to_probe, int *usable_fds, int *already_open)
{
	int		   *fd;
	int			size;
	int			used = 0;
	int			highestfd = 0;
	int			j;

#ifdef HAVE_GETRLIMIT
	struct rlimit rlim;
	int			getrlimit_status;
#endif

	size = 1024;
	fd = (int *) palloc(size * sizeof(int));

#ifdef HAVE_GETRLIMIT
#ifdef RLIMIT_NOFILE			/* most platforms use RLIMIT_NOFILE */
	getrlimit_status = getrlimit(RLIMIT_NOFILE, &rlim);
#else							/* but BSD doesn't ... */
	getrlimit_status = getrlimit(RLIMIT_OFILE, &rlim);
#endif   /* RLIMIT_NOFILE */
	if (getrlimit_status != 0)
		ereport(WARNING, (errmsg("getrlimit failed: %m")));
#endif   /* HAVE_GETRLIMIT */

	/* dup until failure or probe limit reached */
	for (;;)
	{
		int			thisfd;

#ifdef HAVE_GETRLIMIT

		/*
		 * don't go beyond RLIMIT_NOFILE; causes irritating kernel logs on
		 * some platforms
		 */
		if (getrlimit_status == 0 && highestfd >= rlim.rlim_cur - 1)
			break;
#endif

		thisfd = dup(0);
		if (thisfd < 0)
		{
			/* Expect EMFILE or ENFILE, else it's fishy */
			if (errno != EMFILE && errno != ENFILE)
				elog(WARNING, "dup(0) failed after %d successes: %m", used);
			break;
		}

		if (used >= size)
		{
			size *= 2;
			fd = (int *) repalloc(fd, size * sizeof(int));
		}
		fd[used++] = thisfd;

		if (highestfd < thisfd)
			highestfd = thisfd;

		if (used >= max_to_probe)
			break;
	}

	/* release the files we opened */
	for (j = 0; j < used; j++)
		close(fd[j]);

	pfree(fd);

	/*
	 * Return results.  usable_fds is just the number of successful dups. We
	 * assume that the system limit is highestfd+1 (remember 0 is a legal FD
	 * number) and so already_open is highestfd+1 - usable_fds.
	 */
	*usable_fds = used;
	*already_open = highestfd + 1 - used;
}

/*
 * set_max_safe_fds
 *		Determine number of filedescriptors that fd.c is allowed to use
 */
void
set_max_safe_fds(void)
{
	int			usable_fds;
	int			already_open;

	/*----------
	 * We want to set max_safe_fds to
	 *			MIN(usable_fds, max_files_per_process - already_open)
	 * less the slop factor for files that are opened without consulting
	 * fd.c.  This ensures that we won't exceed either max_files_per_process
	 * or the experimentally-determined EMFILE limit.
	 *----------
	 */
	count_usable_fds(max_files_per_process,
					 &usable_fds, &already_open);

	max_safe_fds = Min(usable_fds, max_files_per_process - already_open);

	/*
	 * Take off the FDs reserved for system() etc.
	 */
	max_safe_fds -= NUM_RESERVED_FDS;

	/*
	 * Make sure we still have enough to get by.
	 */
	if (max_safe_fds < FD_MINFREE)
		ereport(FATAL,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("insufficient file descriptors available to start server process"),
				 errdetail("System allows %d, we need at least %d.",
						   max_safe_fds + NUM_RESERVED_FDS,
						   FD_MINFREE + NUM_RESERVED_FDS)));

	elog(DEBUG2, "max_safe_fds = %d, usable_fds = %d, already_open = %d",
		 max_safe_fds, usable_fds, already_open);
}

/*
 * BasicOpenFile --- same as open(2) except can free other FDs if needed
 *
 * This is exported for use by places that really want a plain kernel FD,
 * but need to be proof against running out of FDs.  Once an FD has been
 * successfully returned, it is the caller's responsibility to ensure that
 * it will not be leaked on ereport()!	Most users should *not* call this
 * routine directly, but instead use the VFD abstraction level, which
 * provides protection against descriptor leaks as well as management of
 * files that need to be open for more than a short period of time.
 *
 * Ideally this should be the *only* direct call of open() in the backend.
 * In practice, the postmaster calls open() directly, and there are some
 * direct open() calls done early in backend startup.  Those are OK since
 * this module wouldn't have any open files to close at that point anyway.
 */
int
BasicOpenFile(FileName fileName, int fileFlags, int fileMode)
{
	int			fd;

tryAgain:
	fd = open(fileName, fileFlags, fileMode);

	if (fd >= 0)
		return fd;				/* success! */

	if (errno == EMFILE || errno == ENFILE)
	{
		int			save_errno = errno;

		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("out of file descriptors: %m; release and retry")));
		errno = 0;
		if (ReleaseLruFile())
			goto tryAgain;
		errno = save_errno;
	}

	return -1;					/* failure */
}

#if defined(FDDEBUG)

static void
_dump_lru(void)
{
	int			mru = VfdCache[0].lruLessRecently;
	Vfd		   *vfdP = &VfdCache[mru];
	char		buf[2048];

	snprintf(buf, sizeof(buf), "LRU: MOST %d ", mru);
	while (mru != 0)
	{
		mru = vfdP->lruLessRecently;
		vfdP = &VfdCache[mru];
		snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "%d ", mru);
	}
	snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "LEAST");
	elog(LOG, "%s", buf);
}
#endif   /* FDDEBUG */

static void
Delete(File file)
{
	Vfd		   *vfdP;

	Assert(file != 0);

	DO_DB(elog(LOG, "Delete %d (%s)",
			   file, VfdCache[file].fileName));
	DO_DB(_dump_lru());

	vfdP = &VfdCache[file];

	VfdCache[vfdP->lruLessRecently].lruMoreRecently = vfdP->lruMoreRecently;
	VfdCache[vfdP->lruMoreRecently].lruLessRecently = vfdP->lruLessRecently;

	DO_DB(_dump_lru());
}

static void
LruDelete(File file)
{
	Vfd		   *vfdP;

	Assert(file != 0);

	DO_DB(elog(LOG, "LruDelete %d (%s)",
			   file, VfdCache[file].fileName));

	vfdP = &VfdCache[file];

	/* delete the vfd record from the LRU ring */
	Delete(file);

	/* save the seek position */
	vfdP->seekPos = lseek(vfdP->fd, (off_t) 0, SEEK_CUR);
	Assert(vfdP->seekPos != (off_t) -1);

	/* close the file */
	if (close(vfdP->fd))
		elog(ERROR, "could not close file \"%s\": %m", vfdP->fileName);

	--nfile;
	vfdP->fd = VFD_CLOSED;
}

static void
Insert(File file)
{
	Vfd		   *vfdP;

	Assert(file != 0);

	DO_DB(elog(LOG, "Insert %d (%s)",
			   file, VfdCache[file].fileName));
	DO_DB(_dump_lru());

	vfdP = &VfdCache[file];

	vfdP->lruMoreRecently = 0;
	vfdP->lruLessRecently = VfdCache[0].lruLessRecently;
	VfdCache[0].lruLessRecently = file;
	VfdCache[vfdP->lruLessRecently].lruMoreRecently = file;

	DO_DB(_dump_lru());
}

/* returns 0 on success, -1 on re-open failure (with errno set) */
static int
LruInsert(File file)
{
	Vfd		   *vfdP;

	Assert(file != 0);

	DO_DB(elog(LOG, "LruInsert %d (%s)",
			   file, VfdCache[file].fileName));

	vfdP = &VfdCache[file];

	if (FileIsNotOpen(file))
	{
		/* Close excess kernel FDs. */
		ReleaseLruFiles();

		/*
		 * The open could still fail for lack of file descriptors, eg due to
		 * overall system file table being full.  So, be prepared to release
		 * another FD if necessary...
		 */
		vfdP->fd = BasicOpenFile(vfdP->fileName, vfdP->fileFlags,
								 vfdP->fileMode);
		if (vfdP->fd < 0)
		{
			DO_DB(elog(LOG, "RE_OPEN FAILED: %d", errno));
			return -1;
		}
		else
		{
			DO_DB(elog(LOG, "RE_OPEN SUCCESS"));
			++nfile;
		}

		/* seek to the right position */
		if (vfdP->seekPos != (off_t) 0)
		{
			off_t returnValue PG_USED_FOR_ASSERTS_ONLY;

			returnValue = lseek(vfdP->fd, vfdP->seekPos, SEEK_SET);
			Assert(returnValue != (off_t) -1);
		}
	}

	/*
	 * put it at the head of the Lru ring
	 */

	Insert(file);

	return 0;
}

/*
 * Release one kernel FD by closing the least-recently-used VFD.
 */
static bool
ReleaseLruFile(void)
{
	DO_DB(elog(LOG, "ReleaseLruFile. Opened %d", nfile));

	if (nfile > 0)
	{
		/*
		 * There are opened files and so there should be at least one used vfd
		 * in the ring.
		 */
		Assert(VfdCache[0].lruMoreRecently != 0);
		LruDelete(VfdCache[0].lruMoreRecently);
		return true;			/* freed a file */
	}
	return false;				/* no files available to free */
}

/*
 * Release kernel FDs as needed to get under the max_safe_fds limit.
 * After calling this, it's OK to try to open another file.
 */
static void
ReleaseLruFiles(void)
{
	while (nfile + numAllocatedDescs >= max_safe_fds)
	{
		if (!ReleaseLruFile())
			break;
	}
}

static File
AllocateVfd(void)
{
	Index		i;
	File		file;

	DO_DB(elog(LOG, "AllocateVfd. Size %zu", SizeVfdCache));

	Assert(SizeVfdCache > 0);	/* InitFileAccess not called? */

	if (VfdCache[0].nextFree == 0)
	{
		/*
		 * The free list is empty so it is time to increase the size of the
		 * array.  We choose to double it each time this happens. However,
		 * there's not much point in starting *real* small.
		 */
		Size		newCacheSize = SizeVfdCache * 2;
		Vfd		   *newVfdCache;

		if (newCacheSize < 32)
			newCacheSize = 32;

		/*
		 * Be careful not to clobber VfdCache ptr if realloc fails.
		 */
		newVfdCache = (Vfd *) realloc(VfdCache, sizeof(Vfd) * newCacheSize);
		if (newVfdCache == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		VfdCache = newVfdCache;

		/*
		 * Initialize the new entries and link them into the free list.
		 */
		for (i = SizeVfdCache; i < newCacheSize; i++)
		{
			MemSet((char *) &(VfdCache[i]), 0, sizeof(Vfd));
			VfdCache[i].nextFree = i + 1;
			VfdCache[i].fd = VFD_CLOSED;
                        VfdCache[i].pm_list_idx = -1;
		}
		VfdCache[newCacheSize - 1].nextFree = 0;
		VfdCache[0].nextFree = SizeVfdCache;

		/*
		 * Record the new size
		 */
		SizeVfdCache = newCacheSize;
	}

	file = VfdCache[0].nextFree;

	VfdCache[0].nextFree = VfdCache[file].nextFree;

	return file;
}

static void
FreeVfd(File file)
{
        int i;
	Vfd		   *vfdP = &VfdCache[file];

	DO_DB(elog(LOG, "FreeVfd: %d (%s)",
			   file, vfdP->fileName ? vfdP->fileName : ""));

	if (vfdP->fileName != NULL)
	{
		free(vfdP->fileName);
		vfdP->fileName = NULL;
	}
	vfdP->fdstate = 0x0;
        for (i = 0; i <= vfdP->pm_list_idx; i++) {
            vfdP->pm_ptr_list[i] = NULL;
	    vfdP->pm_size_list[i] = 0;
        }
        vfdP->pm_list_idx = -1;

	vfdP->nextFree = VfdCache[0].nextFree;
	VfdCache[0].nextFree = file;
}

/* returns 0 on success, -1 on re-open failure (with errno set) */
static int
FileAccess(File file)
{
	int			returnValue;

	DO_DB(elog(LOG, "FileAccess %d (%s)",
			   file, VfdCache[file].fileName));

	/*
	 * Is the file open?  If not, open it and put it at the head of the LRU
	 * ring (possibly closing the least recently used file to get an FD).
	 */

	if (FileIsNotOpen(file))
	{
		returnValue = LruInsert(file);
		if (returnValue != 0)
			return returnValue;
	}
	else if (VfdCache[0].lruLessRecently != file)
	{
		/*
		 * We now know that the file is open and that it is not the last one
		 * accessed, so we need to move it to the head of the Lru ring.
		 */

		Delete(file);
		Insert(file);
	}

	return 0;
}

/*
 *	Called when we get a shared invalidation message on some relation.
 */
#ifdef NOT_USED
void
FileInvalidate(File file)
{
	Assert(FileIsValid(file));
	if (!FileIsNotOpen(file))
		LruDelete(file);
}
#endif

/*
 * open a file in an arbitrary directory
 *
 * NB: if the passed pathname is relative (which it usually is),
 * it will be interpreted relative to the process' working directory
 * (which should always be $PGDATA when this code is running).
 */
File
PathNameOpenFile(FileName fileName, int fileFlags, int fileMode)
{
	char	   *fnamecopy;
	File		file;
	Vfd		   *vfdP;
        struct stat sbuf; 

	DO_DB(elog(LOG, "PathNameOpenFile: %s %x %o",
			   fileName, fileFlags, fileMode));

	/*
	 * We need a malloc'd copy of the file name; fail cleanly if no room.
	 */
	fnamecopy = strdup(fileName);
	if (fnamecopy == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	file = AllocateVfd();
	vfdP = &VfdCache[file];

	/* Close excess kernel FDs. */
	ReleaseLruFiles();

	vfdP->fd = BasicOpenFile(fileName, fileFlags, fileMode);

	if (vfdP->fd < 0)
	{
		int			save_errno = errno;

		FreeVfd(file);
		free(fnamecopy);
		errno = save_errno;
		return -1;
	}

	DO_DB(elog(LOG, "PathNameOpenFile File is Open: %s %x %o",
			   fileName, fileFlags, fileMode));
        /* 
         * AAS: memory map file 
         */
        Assert(vfdP->pm_list_idx == -1);
        if (fstat(vfdP->fd, &sbuf) != 0) {
		int			save_errno = errno;

		FreeVfd(file);
		free(fnamecopy);
		errno = save_errno;
		return -1;
        } 
        vfdP->pm_size_list[0] = sbuf.st_size;

        if (vfdP->pm_size_list[0] != 0) {
            vfdP->pm_ptr_list[0] = mmap(NULL, vfdP->pm_size_list[0], PROT_READ | PROT_WRITE, MAP_SHARED, vfdP->fd, 0);
            if (vfdP->pm_ptr_list[0] == MAP_FAILED) {
                    DO_DB(elog(LOG, "PathNameOpenFile ERROR mapping file: %s %x %o",
                               fileName, fileFlags, fileMode));
                    int			save_errno = errno;

                    FreeVfd(file);
                    free(fnamecopy);
                    errno = save_errno;
                    return -1;
            }
            //Naveed
            //==============================

            //preparing arguments for work function


            /*
            int Limit=1024;//1 KB
            struct HT_args arg;
            arg.startAddr=vfdP->pm_size_list[0];
            arg.direction=DIRECTION;
            if((vfdP->pm_size_list[0])<=Limit)
                arg.NumberOfBytes=(vfdP->pm_size_list[0]);
            else
                arg.NumberOfBytes=Limit;
            */



            //printf("SEND: vfdP->pm_size_list[0]=%d arg.NumberOfBytes=%d\n",(vfdP->pm_size_list[0]),arg.NumberOfBytes);
            //arg.NumberOfBytes=1024;//1 KB
            //printf("SENT: arg.startAddr=%p arg.direction=%d arg.NumberOfBytes=%d\n",arg.startAddr,arg.direction,arg.NumberOfBytes);
/*
            //adding work to the pool
            printf("adding work to thread pool\n");
            thpool_add_work(thpoolGloabl1, (void*)prefetchData, &arg);
            //thpool_wait(thpoolGloabl1);
            //sleep(2);
            */

           // printf("Before thpoolGlobal1=%d\n",(int)thpoolGloabl1);
            //printf("Making threadpool with 4 threads\n");



            /*
            if(thpoolGloabl1==NULL)
                thpoolGloabl1 = thpool_init(10);
            */



            //printf("After thpoolGlobal1=%d\n",(int)thpoolGloabl1);

/*
            pid_t pid;
            if ((pid = getpid()) < 0) {
                  perror("FathNameOpenFile unable to get pid\n");
                }
            else {
                  printf("FathNameOpenFile The process id is %d\n", pid);
                }
*/
            //printf("Adding 1 task to threadpool\n");
/*
            int i;
            for (i=0; i<20; i++){*/
                //thpool_add_work(thpoolGloabl1, &task1, NULL);
            //thpool_add_work(thpoolGloabl1, (void*)prefetchData, &arg);
                /*thpool_add_work(thpoolGloabl1, &task2, NULL);
                };*/

            /*
            thpool_wait(thpoolGloabl1);
            printf("Killing threadpool\n");
            thpool_destroy(thpoolGloabl1);
            */

/*
            int err;

            //variable to contain thread id
            pthread_t thread_id;

            //pinning to a core
            pthread_attr_t attr;//define attribute
            pthread_attr_init(&attr);//initialize with defaylt values
            int speid=2;//the cpu we want to use
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);//clear cpuset
            CPU_SET(speid, &cpuset);//set CPU 2 on cpuset
            pthread_attr_setaffinity_np(&attr,sizeof(cpu_set_t), &cpuset);

            //passing arguments to thread
            struct HT_args arg;
            arg.startAddr=vfdP->pm_size_list[0];
            arg.direction=1;
            arg.NumberOfBytes=vfdP->pm_size_list[0];


            //invoke thread
            err = pthread_create(&thread_id,&attr, &HelperThread,&arg);
            if (err != 0)
                return -1;

            //pthread_join(thread_id, NULL);
*/
            //==============================
            vfdP->pm_list_idx++;
        } else {
            /* Cannot mmap at this point, file must have size > 0 */
            vfdP->pm_ptr_list[0] = NULL;
        }

	++nfile;
	DO_DB(elog(LOG, "PathNameOpenFile: success %d",
			   vfdP->fd));

	Insert(file);

	vfdP->fileName = fnamecopy;
	/* Saved flags are adjusted to be OK for re-opening file */
	vfdP->fileFlags = fileFlags & ~(O_CREAT | O_TRUNC | O_EXCL);
	vfdP->fileMode = fileMode;
	vfdP->seekPos = 0;
	vfdP->fileSize = 0;
	vfdP->fdstate = 0x0;
	vfdP->resowner = NULL;

	return file;
}

/*
 * Open a temporary file that will disappear when we close it.
 *
 * This routine takes care of generating an appropriate tempfile name.
 * There's no need to pass in fileFlags or fileMode either, since only
 * one setting makes any sense for a temp file.
 *
 * Unless interXact is true, the file is remembered by CurrentResourceOwner
 * to ensure it's closed and deleted when it's no longer needed, typically at
 * the end-of-transaction. In most cases, you don't want temporary files to
 * outlive the transaction that created them, so this should be false -- but
 * if you need "somewhat" temporary storage, this might be useful. In either
 * case, the file is removed when the File is explicitly closed.
 */
File
OpenTemporaryFile(bool interXact)
{
	File		file = 0;

	/*
	 * If some temp tablespace(s) have been given to us, try to use the next
	 * one.  If a given tablespace can't be found, we silently fall back to
	 * the database's default tablespace.
	 *
	 * BUT: if the temp file is slated to outlive the current transaction,
	 * force it into the database's default tablespace, so that it will not
	 * pose a threat to possible tablespace drop attempts.
	 */
	if (numTempTableSpaces > 0 && !interXact)
	{
		Oid			tblspcOid = GetNextTempTableSpace();

		if (OidIsValid(tblspcOid))
			file = OpenTemporaryFileInTablespace(tblspcOid, false);
	}

	/*
	 * If not, or if tablespace is bad, create in database's default
	 * tablespace.  MyDatabaseTableSpace should normally be set before we get
	 * here, but just in case it isn't, fall back to pg_default tablespace.
	 */
	if (file <= 0)
		file = OpenTemporaryFileInTablespace(MyDatabaseTableSpace ?
											 MyDatabaseTableSpace :
											 DEFAULTTABLESPACE_OID,
											 true);

	/* Mark it for deletion at close */
	VfdCache[file].fdstate |= FD_TEMPORARY;

	/* Register it with the current resource owner */
	if (!interXact)
	{
		VfdCache[file].fdstate |= FD_XACT_TEMPORARY;

		ResourceOwnerEnlargeFiles(CurrentResourceOwner);
		ResourceOwnerRememberFile(CurrentResourceOwner, file);
		VfdCache[file].resowner = CurrentResourceOwner;

		/* ensure cleanup happens at eoxact */
		have_xact_temporary_files = true;
	}

	return file;
}

/*
 * Open a temporary file in a specific tablespace.
 * Subroutine for OpenTemporaryFile, which see for details.
 */
static File
OpenTemporaryFileInTablespace(Oid tblspcOid, bool rejectError)
{
	char		tempdirpath[MAXPGPATH];
	char		tempfilepath[MAXPGPATH];
	File		file;

	/*
	 * Identify the tempfile directory for this tablespace.
	 *
	 * If someone tries to specify pg_global, use pg_default instead.
	 */
	if (tblspcOid == DEFAULTTABLESPACE_OID ||
		tblspcOid == GLOBALTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		snprintf(tempdirpath, sizeof(tempdirpath), "base/%s",
				 PG_TEMP_FILES_DIR);
	}
	else
	{
		/* All other tablespaces are accessed via symlinks */
		snprintf(tempdirpath, sizeof(tempdirpath), "pg_tblspc/%u/%s/%s",
				 tblspcOid, TABLESPACE_VERSION_DIRECTORY, PG_TEMP_FILES_DIR);
	}

	/*
	 * Generate a tempfile name that should be unique within the current
	 * database instance.
	 */
	snprintf(tempfilepath, sizeof(tempfilepath), "%s/%s%d.%ld",
			 tempdirpath, PG_TEMP_FILE_PREFIX, MyProcPid, tempFileCounter++);

	/*
	 * Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
	 * temp file that can be reused.
	 */
	file = PathNameOpenFile(tempfilepath,
							O_RDWR | O_CREAT | O_TRUNC | PG_BINARY,
							0600);
	if (file <= 0)
	{
		/*
		 * We might need to create the tablespace's tempfile directory, if no
		 * one has yet done so.
		 *
		 * Don't check for error from mkdir; it could fail if someone else
		 * just did the same thing.  If it doesn't work then we'll bomb out on
		 * the second create attempt, instead.
		 */
		mkdir(tempdirpath, S_IRWXU);

		file = PathNameOpenFile(tempfilepath,
								O_RDWR | O_CREAT | O_TRUNC | PG_BINARY,
								0600);
		if (file <= 0 && rejectError)
			elog(ERROR, "could not create temporary file \"%s\": %m",
				 tempfilepath);
	}

	return file;
}

/*
 * close a file when done with it
 */
void
FileClose(File file)
{
	Vfd		   *vfdP;
        int i;
        off_t prev_map_size;

	Assert(FileIsValid(file));

	DO_DB(elog(LOG, "FileClose: %d (%s)",
			   file, VfdCache[file].fileName));

	vfdP = &VfdCache[file];

        /* AAS: We cannot unmap all current mappings because postgres does not release the current
         * shared buffers for this file. This causes all sorts of problems and is the main reason to leave
         * the mappings open. Ideally the buffer manager should clean up before calling FileClose */
        if (FileIsMapped(file)) {
            /*prev_map_size = 0;*/
            for (i = 0; i <= vfdP->pm_list_idx; i++) {

                /*if (munmap(vfdP->pm_ptr_list[i], vfdP->pm_size_list[i] - prev_map_size) != 0) {*/
                    /*elog(ERROR, "could not unmap file \"%s\": %m", vfdP->fileName);*/
                /*}*/
                /*prev_map_size = vfdP->pm_size_list[i];*/
                vfdP->pm_ptr_list[i] = NULL;
                vfdP->pm_size_list[i] = 0;
            }
            vfdP->pm_list_idx = -1;
        }

	if (!FileIsNotOpen(file))
	{
		/* remove the file from the lru ring */
		Delete(file);

		/* close the file */
		if (close(vfdP->fd))
			elog(ERROR, "could not close file \"%s\": %m", vfdP->fileName);

		--nfile;
		vfdP->fd = VFD_CLOSED;
	}

	/*
	 * Delete the file if it was temporary, and make a log entry if wanted
	 */
	if (vfdP->fdstate & FD_TEMPORARY)
	{
		struct stat filestats;
		int			stat_errno;

		/*
		 * If we get an error, as could happen within the ereport/elog calls,
		 * we'll come right back here during transaction abort.  Reset the
		 * flag to ensure that we can't get into an infinite loop.  This code
		 * is arranged to ensure that the worst-case consequence is failing to
		 * emit log message(s), not failing to attempt the unlink.
		 */
		vfdP->fdstate &= ~FD_TEMPORARY;

		/* Subtract its size from current usage (do first in case of error) */
		temporary_files_size -= vfdP->fileSize;
		vfdP->fileSize = 0;

		/* first try the stat() */
		if (stat(vfdP->fileName, &filestats))
			stat_errno = errno;
		else
			stat_errno = 0;

		/* in any case do the unlink */
		if (unlink(vfdP->fileName))
			elog(LOG, "could not unlink file \"%s\": %m", vfdP->fileName);

		/* and last report the stat results */
		if (stat_errno == 0)
		{
			pgstat_report_tempfile(filestats.st_size);

			if (log_temp_files >= 0)
			{
				if ((filestats.st_size / 1024) >= log_temp_files)
					ereport(LOG,
							(errmsg("temporary file: path \"%s\", size %lu",
									vfdP->fileName,
									(unsigned long) filestats.st_size)));
			}
		}
		else
		{
			errno = stat_errno;
			elog(LOG, "could not stat file \"%s\": %m", vfdP->fileName);
		}
	}

	/* Unregister it from the resource owner */
	if (vfdP->resowner)
		ResourceOwnerForgetFile(vfdP->resowner, file);

	/*
	 * Return the Vfd slot to the free list
	 */
	FreeVfd(file);
}

/*
 * FilePrefetch - initiate asynchronous read of a given range of the file.
 * The logical seek position is unaffected.
 *
 * Currently the only implementation of this function is using posix_fadvise
 * which is the simplest standardized interface that accomplishes this.
 * We could add an implementation using libaio in the future; but note that
 * this API is inappropriate for libaio, which wants to have a buffer provided
 * to read into.
 */
int
FilePrefetch(File file, off_t offset, int amount)
{
/*#if defined(USE_POSIX_FADVISE) && defined(POSIX_FADV_WILLNEED)*/
	/*int			returnCode;*/

	/*Assert(FileIsValid(file));*/

	/*DO_DB(elog(LOG, "FilePrefetch: %d (%s) " INT64_FORMAT " %d",*/
			   /*file, VfdCache[file].fileName,*/
			   /*(int64) offset, amount));*/

	/*returnCode = FileAccess(file);*/
	/*if (returnCode < 0)*/
		/*return returnCode;*/

	/*returnCode = posix_fadvise(VfdCache[file].fd, offset, amount,*/
							   /*POSIX_FADV_WILLNEED);*/

	/*return returnCode;*/
/*#else*/
	Assert(FileIsValid(file));
	return 0;
/*#endif*/
}

int
FileRead(File file, char **buffer, int amount)
{


	int			returnCode;
        int i;
        off_t delta;

	Assert(FileIsValid(file));
	Assert(FileIsMapped(file));

	DO_DB(elog(LOG, "FileRead: %d (%s) " INT64_FORMAT " %d %p",
			   file, VfdCache[file].fileName,
			   (int64) VfdCache[file].seekPos,
			   amount, buffer));

	returnCode = FileAccess(file);
	if (returnCode < 0)
		return returnCode;

retry:
	/*returnCode = read(VfdCache[file].fd, buffer, amount);*/

        /* 
         * AAS: Check if amount is higher than the actual amount of bytes left
         * in file to read. This can happen in temporary files.
         */
        if (VfdCache[file].seekPos + amount > VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx]) {
            amount = VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx] - VfdCache[file].seekPos;
        }

        /* 
         * AAS: Redirect pointer to PM file.
         */
        /*for (i = 0; i <= VfdCache[file].pm_list_idx; i++) {*/
            /*if (VfdCache[file].pm_size_list[i] > VfdCache[file].seekPos) {*/
                /*break;*/
            /*}*/
        /*}*/
        i = getIdxFromSeekpos(file);
        Assert(i <= VfdCache[file].pm_list_idx && i >= 0);
        if (i == 0) delta = VfdCache[file].seekPos;
        else delta = VfdCache[file].seekPos - VfdCache[file].pm_size_list[i-1];
        Assert(delta >= 0);
        *buffer = (char*) VfdCache[file].pm_ptr_list[i] + delta;

        //Naveed_Ext
        //===============================================
        //initilize thread (if already not initialized)
        if(gloablThrdeadID==NULL)
            {
            //initialize mutex and cond var
            pthread_mutex_init(&fetch_mutex, NULL);
            pthread_cond_init (&fetch_cv, NULL);
            //create thread
            initThread(&gloablThrdeadID);
            //initailize all other variables
            push_Index=-1;
            pull_Index=-1;            
            remJobs=0;
            prevFetchStartAdr=NULL;
            prevFetchEndAdr=NULL;
            //setting last element of tempBuffer to NULL to avoid segmentation fault
            tempBuffer[BufferSize-1]=0;
            }


        //assign work to thread
        //prepare argument
        arg.srcAddr=(char*) VfdCache[file].pm_ptr_list[i] + delta;//start copying from NVM location
        arg.BlkSize=((VfdCache[file].pm_size_list[i])-delta);//remaining unfected size of file mapping

        //prepare job
        job.arg=&arg;
        job.argPlaced=1;//indicates that a valid arg is placed


        pthread_mutex_lock(&fetch_mutex);           //lock mutex
        push_Index=(++push_Index)%jobQueueSize;      //increment queue push index
        ++remJobs;                                  //increment number of jobs waiting to served
        jobArray[push_Index]=job;                   //place job in job Queue
        pthread_cond_signal(&fetch_cv);             //signal the cond var
        pthread_mutex_unlock(&fetch_mutex);         //unlock mutex


        //===============================================




//        __builtin_prefetch((void*) ((char*)*buffer));
//        __builtin_prefetch((void*) ((char*)*buffer) + 64);


        returnCode = amount;
	if (returnCode >= 0)
		VfdCache[file].seekPos += returnCode;
	else
	{
		/*
		 * Windows may run out of kernel buffers and return "Insufficient
		 * system resources" error.  Wait a bit and retry to solve it.
		 *
		 * It is rumored that EINTR is also possible on some Unix filesystems,
		 * in which case immediate retry is indicated.
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;

		/* Trouble, so assume we don't know the file position anymore */
		VfdCache[file].seekPos = FileUnknownPos;
	}

	return returnCode;
}

int
FileWrite(File file, char *buffer, int amount)
{
	int			returnCode;
        int i;
        off_t delta;
        off_t offset;
        char* map_ptr;

	Assert(FileIsValid(file));

	DO_DB(elog(LOG, "FileWrite: %d (%s) " INT64_FORMAT " %d %p",
			   file, VfdCache[file].fileName,
			   (int64) VfdCache[file].seekPos,
			   amount, buffer));

	returnCode = FileAccess(file);
	if (returnCode < 0)
		return returnCode;

	/*
	 * If enforcing temp_file_limit and it's a temp file, check to see if the
	 * write would overrun temp_file_limit, and throw error if so.  Note: it's
	 * really a modularity violation to throw error here; we should set errno
	 * and return -1.  However, there's no way to report a suitable error
	 * message if we do that.  All current callers would just throw error
	 * immediately anyway, so this is safe at present.
	 */
	if (temp_file_limit >= 0 && (VfdCache[file].fdstate & FD_TEMPORARY))
	{
		off_t		newPos = VfdCache[file].seekPos + amount;

		if (newPos > VfdCache[file].fileSize)
		{
			uint64		newTotal = temporary_files_size;

			newTotal += newPos - VfdCache[file].fileSize;
			if (newTotal > (uint64) temp_file_limit * (uint64) 1024)
				ereport(ERROR,
						(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("temporary file size exceeds temp_file_limit (%dkB)",
						temp_file_limit)));
		}
	}

retry:
        /* AAS: Changing to mmap I/O */
        if (FileIsMapped(file)) 
            Assert(VfdCache[file].seekPos <= VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx]);

        if (FileIsMapped(file) && VfdCache[file].seekPos < VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx])
            Assert(VfdCache[file].seekPos+amount <= VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx]);

        if ( !FileIsMapped(file) || VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx] == VfdCache[file].seekPos) {
            if (!FileIsMapped(file)) {
                Assert(VfdCache[file].seekPos == 0);
                offset = amount;
            }else {
                offset = VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx] + amount;
            }
            if (FileTruncate(file, offset) < 0) {
                    ereport(ERROR,
                                    (errcode_for_file_access(),
                                     errmsg("could not truncate file \"%s\": %m",
                                                    VfdCache[file].fileName)));
            }
        }

        Assert(VfdCache[file].seekPos + amount <= VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx]);

        /* AAS: Find the actual map ptr */
        /*for (i = 0; i <= VfdCache[file].pm_list_idx; i++) {*/
            /*if (VfdCache[file].pm_size_list[i] > VfdCache[file].seekPos) {*/
                /*break;*/
            /*}*/
        /*}*/
        i = getIdxFromSeekpos(file);
        Assert(i <= VfdCache[file].pm_list_idx && i >= 0);
        if (i == 0) delta = VfdCache[file].seekPos;
        else delta = VfdCache[file].seekPos - VfdCache[file].pm_size_list[i-1];
        Assert(delta >= 0);
        map_ptr = (char*) VfdCache[file].pm_ptr_list[i] + delta;

        if (memcpy(map_ptr, buffer, amount) != map_ptr) {
            returnCode = -1;
        } else {
            returnCode = amount;
        }

	/*returnCode = write(VfdCache[file].fd, buffer, amount);*/

	/* if write didn't set errno, assume problem is no disk space */
	if (returnCode != amount && errno == 0)
		errno = ENOSPC;

	if (returnCode >= 0)
	{
		VfdCache[file].seekPos += returnCode;

		/* maintain fileSize and temporary_files_size if it's a temp file */
		if (VfdCache[file].fdstate & FD_TEMPORARY)
		{
			off_t		newPos = VfdCache[file].seekPos;

			if (newPos > VfdCache[file].fileSize)
			{
				temporary_files_size += newPos - VfdCache[file].fileSize;
				VfdCache[file].fileSize = newPos;
			}
		}
	}
	else
	{
		/*
		 * See comments in FileRead()
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;

		/* Trouble, so assume we don't know the file position anymore */
		VfdCache[file].seekPos = FileUnknownPos;
	}

	return returnCode;
}

int
FileSync(File file)
{
	int			returnCode;

	Assert(FileIsValid(file));
	Assert(FileIsMapped(file));

	DO_DB(elog(LOG, "FileSync: %d (%s)",
			   file, VfdCache[file].fileName));

	returnCode = FileAccess(file);
	if (returnCode < 0)
		return returnCode;

        /* AAS: Changed the call to a function that uses msync */
	return pg_msync(file);
}

off_t
FileSeek(File file, off_t offset, int whence)
{
        /* AAS: Modified not to use lseek at all, not necessary */
	/*int			returnCode;*/

	Assert(FileIsValid(file));

	DO_DB(elog(LOG, "FileSeek: %d (%s) " INT64_FORMAT " " INT64_FORMAT " %d",
			   file, VfdCache[file].fileName,
			   (int64) VfdCache[file].seekPos,
			   (int64) offset, whence));

	/*if (FileIsNotOpen(file))*/
	/*{*/
		switch (whence)
		{
			case SEEK_SET:
				if (offset < 0)
					elog(ERROR, "invalid seek offset: " INT64_FORMAT,
						 (int64) offset);
				VfdCache[file].seekPos = offset;
				break;
			case SEEK_CUR:
				VfdCache[file].seekPos += offset;
				break;
			case SEEK_END:
				/*returnCode = FileAccess(file);*/
				/*if (returnCode < 0)*/
					/*return returnCode;*/
				/*VfdCache[file].seekPos = lseek(VfdCache[file].fd, offset, whence);*/
                                VfdCache[file].seekPos = VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx] + offset;
				break;
			default:
				elog(ERROR, "invalid whence: %d", whence);
				break;
		}
	/*}*/
	/*else*/
	/*{*/
		/*switch (whence)*/
		/*{*/
			/*case SEEK_SET:*/
				/*if (offset < 0)*/
					/*elog(ERROR, "invalid seek offset: " INT64_FORMAT,*/
						 /*(int64) offset);*/
				/*if (VfdCache[file].seekPos != offset)*/
					/*VfdCache[file].seekPos = lseek(VfdCache[file].fd,*/
												   /*offset, whence);*/
				/*break;*/
			/*case SEEK_CUR:*/
				/*if (offset != 0 || VfdCache[file].seekPos == FileUnknownPos)*/
					/*VfdCache[file].seekPos = lseek(VfdCache[file].fd,*/
												   /*offset, whence);*/
				/*break;*/
			/*case SEEK_END:*/
				/*VfdCache[file].seekPos = lseek(VfdCache[file].fd,*/
											   /*offset, whence);*/
				/*break;*/
			/*default:*/
				/*elog(ERROR, "invalid whence: %d", whence);*/
				/*break;*/
		/*}*/
	/*}*/
	return VfdCache[file].seekPos;
}

/*
 * XXX not actually used but here for completeness
 */
#ifdef NOT_USED
off_t
FileTell(File file)
{
	Assert(FileIsValid(file));
	DO_DB(elog(LOG, "FileTell %d (%s)",
			   file, VfdCache[file].fileName));
	return VfdCache[file].seekPos;
}
#endif

int
FileTruncate(File file, off_t offset)
{
	int			returnCode;
        bool shrink;
        int current_mapping;
        int i;
        int old_map_size, new_map_size;
        off_t start_new_map;
        struct stat sbuf;

	Assert(FileIsValid(file));

	DO_DB(elog(LOG, "FileTruncate %d (%s)",
			   file, VfdCache[file].fileName));

	returnCode = FileAccess(file);
	if (returnCode < 0)
		return returnCode;
        
        /* 
         * AAS: We can never unmap a region of a file until we call FileClose since we cannot
         * guarantee that after truncating the mapping will be done in the same place. For this
         * reason we need to maintain a list of mappings per file.
         */

        /*
         * AAS: First check if we are shrinking the file and unmap as many mappings as necessary.
         * Finally remap if necessary
         */
        if (fstat(VfdCache[file].fd, &sbuf) != 0) {
            Assert(false);
        } 
        if (sbuf.st_size > 0) Assert(FileIsMapped(file));
        else Assert(!FileIsMapped(file));

        shrink = (FileIsMapped(file) && (offset < VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx]));
        if (shrink) {
            for (i = VfdCache[file].pm_list_idx; i >= 0; i--) {
                if (offset == 0 || (i > 0 && offset <= VfdCache[file].pm_size_list[i-1])) {
                    returnCode = munmap(VfdCache[file].pm_ptr_list[i], VfdCache[file].pm_size_list[i]);
                    if (returnCode != 0) return returnCode;

                    VfdCache[file].pm_ptr_list[i] = 0;
                    VfdCache[file].pm_size_list[i] = 0;
                    VfdCache[file].pm_list_idx--;
                } else {
                    //Check if remap
                    Assert(offset <= VfdCache[file].pm_size_list[i]);
                    if (i == 0 || (offset > VfdCache[file].pm_size_list[i-1] && offset != VfdCache[file].pm_size_list[i])) {
                        if (i == 0) {
                            old_map_size = VfdCache[file].pm_size_list[i];
                            new_map_size = offset;
                        } else {
                            old_map_size = VfdCache[file].pm_size_list[i] - VfdCache[file].pm_size_list[i-1];
                            new_map_size = offset - VfdCache[file].pm_size_list[i-1];
                        } 
                        if (mremap(VfdCache[file].pm_ptr_list[i], old_map_size, new_map_size, MREMAP_FIXED, VfdCache[file].pm_ptr_list[i]) == MAP_FAILED)
                            return -1;
                        VfdCache[file].pm_size_list[i] = new_map_size;
                    }
                    break;
                }
            }
        }

	returnCode = ftruncate(VfdCache[file].fd, offset);
	if (returnCode < 0) {
	    return returnCode;
        }

	if (returnCode == 0 && VfdCache[file].fileSize > offset)
	{
		/* adjust our state for truncation of a temp file */
		Assert(VfdCache[file].fdstate & FD_TEMPORARY);
		temporary_files_size -= VfdCache[file].fileSize - offset;
		VfdCache[file].fileSize = offset;
	}

        /*
         * AAS: If we are extending a new mapping needs to be created
         */
        if (!shrink) {
            if(!FileIsMapped(file)) {
                new_map_size = offset;
                start_new_map = 0;
            } else {
                new_map_size = offset - VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx];
                start_new_map = VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx];
            }
            
            VfdCache[file].pm_ptr_list[VfdCache[file].pm_list_idx + 1] = mmap(NULL, new_map_size, PROT_READ | PROT_WRITE, MAP_SHARED, VfdCache[file].fd, start_new_map);
            if (VfdCache[file].pm_ptr_list[VfdCache[file].pm_list_idx + 1] == MAP_FAILED) {
                return -1;
            }
            VfdCache[file].pm_size_list[VfdCache[file].pm_list_idx + 1] = offset;
            VfdCache[file].pm_list_idx++;
        }

	return returnCode;
}

/*
 * Return the pathname associated with an open file.
 *
 * The returned string points to an internal buffer, which is valid until
 * the file is closed.
 */
char *
FilePathName(File file)
{
	Assert(FileIsValid(file));

	return VfdCache[file].fileName;
}


/*
 * Make room for another allocatedDescs[] array entry if needed and possible.
 * Returns true if an array element is available.
 */
static bool
reserveAllocatedDesc(void)
{
	AllocateDesc *newDescs;
	int			newMax;

	/* Quick out if array already has a free slot. */
	if (numAllocatedDescs < maxAllocatedDescs)
		return true;

	/*
	 * If the array hasn't yet been created in the current process, initialize
	 * it with FD_MINFREE / 2 elements.  In many scenarios this is as many as
	 * we will ever need, anyway.  We don't want to look at max_safe_fds
	 * immediately because set_max_safe_fds() may not have run yet.
	 */
	if (allocatedDescs == NULL)
	{
		newMax = FD_MINFREE / 2;
		newDescs = (AllocateDesc *) malloc(newMax * sizeof(AllocateDesc));
		/* Out of memory already?  Treat as fatal error. */
		if (newDescs == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		allocatedDescs = newDescs;
		maxAllocatedDescs = newMax;
		return true;
	}

	/*
	 * Consider enlarging the array beyond the initial allocation used above.
	 * By the time this happens, max_safe_fds should be known accurately.
	 *
	 * We mustn't let allocated descriptors hog all the available FDs, and in
	 * practice we'd better leave a reasonable number of FDs for VFD use.  So
	 * set the maximum to max_safe_fds / 2.  (This should certainly be at
	 * least as large as the initial size, FD_MINFREE / 2.)
	 */
	newMax = max_safe_fds / 2;
	if (newMax > maxAllocatedDescs)
	{
		newDescs = (AllocateDesc *) realloc(allocatedDescs,
											newMax * sizeof(AllocateDesc));
		/* Treat out-of-memory as a non-fatal error. */
		if (newDescs == NULL)
			return false;
		allocatedDescs = newDescs;
		maxAllocatedDescs = newMax;
		return true;
	}

	/* Can't enlarge allocatedDescs[] any more. */
	return false;
}

/*
 * Routines that want to use stdio (ie, FILE*) should use AllocateFile
 * rather than plain fopen().  This lets fd.c deal with freeing FDs if
 * necessary to open the file.  When done, call FreeFile rather than fclose.
 *
 * Note that files that will be open for any significant length of time
 * should NOT be handled this way, since they cannot share kernel file
 * descriptors with other files; there is grave risk of running out of FDs
 * if anyone locks down too many FDs.  Most callers of this routine are
 * simply reading a config file that they will read and close immediately.
 *
 * fd.c will automatically close all files opened with AllocateFile at
 * transaction commit or abort; this prevents FD leakage if a routine
 * that calls AllocateFile is terminated prematurely by ereport(ERROR).
 *
 * Ideally this should be the *only* direct call of fopen() in the backend.
 */
FILE *
AllocateFile(const char *name, const char *mode)
{
	FILE	   *file;

	DO_DB(elog(LOG, "AllocateFile: Allocated %d (%s)",
			   numAllocatedDescs, name));

	/* Can we allocate another non-virtual FD? */
	if (!reserveAllocatedDesc())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("exceeded maxAllocatedDescs (%d) while trying to open file \"%s\"",
						maxAllocatedDescs, name)));

	/* Close excess kernel FDs. */
	ReleaseLruFiles();

TryAgain:
	if ((file = fopen(name, mode)) != NULL)
	{
		AllocateDesc *desc = &allocatedDescs[numAllocatedDescs];

		desc->kind = AllocateDescFile;
		desc->desc.file = file;
		desc->create_subid = GetCurrentSubTransactionId();
		numAllocatedDescs++;
		return desc->desc.file;
	}

	if (errno == EMFILE || errno == ENFILE)
	{
		int			save_errno = errno;

		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("out of file descriptors: %m; release and retry")));
		errno = 0;
		if (ReleaseLruFile())
			goto TryAgain;
		errno = save_errno;
	}

	return NULL;
}


/*
 * Like AllocateFile, but returns an unbuffered fd like open(2)
 */
int
OpenTransientFile(FileName fileName, int fileFlags, int fileMode)
{
	int			fd;

	DO_DB(elog(LOG, "OpenTransientFile: Allocated %d (%s)",
			   numAllocatedDescs, fileName));

	/* Can we allocate another non-virtual FD? */
	if (!reserveAllocatedDesc())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("exceeded maxAllocatedDescs (%d) while trying to open file \"%s\"",
						maxAllocatedDescs, fileName)));

	/* Close excess kernel FDs. */
	ReleaseLruFiles();

	fd = BasicOpenFile(fileName, fileFlags, fileMode);

	if (fd >= 0)
	{
		AllocateDesc *desc = &allocatedDescs[numAllocatedDescs];

		desc->kind = AllocateDescRawFD;
		desc->desc.fd = fd;
		desc->create_subid = GetCurrentSubTransactionId();
		numAllocatedDescs++;

		return fd;
	}

	return -1;					/* failure */
}

/*
 * Routines that want to initiate a pipe stream should use OpenPipeStream
 * rather than plain popen().  This lets fd.c deal with freeing FDs if
 * necessary.  When done, call ClosePipeStream rather than pclose.
 */
FILE *
OpenPipeStream(const char *command, const char *mode)
{
	FILE	   *file;

	DO_DB(elog(LOG, "OpenPipeStream: Allocated %d (%s)",
			   numAllocatedDescs, command));

	/* Can we allocate another non-virtual FD? */
	if (!reserveAllocatedDesc())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("exceeded maxAllocatedDescs (%d) while trying to execute command \"%s\"",
						maxAllocatedDescs, command)));

	/* Close excess kernel FDs. */
	ReleaseLruFiles();

TryAgain:
	fflush(stdout);
	fflush(stderr);
	errno = 0;
	if ((file = popen(command, mode)) != NULL)
	{
		AllocateDesc *desc = &allocatedDescs[numAllocatedDescs];

		desc->kind = AllocateDescPipe;
		desc->desc.file = file;
		desc->create_subid = GetCurrentSubTransactionId();
		numAllocatedDescs++;
		return desc->desc.file;
	}

	if (errno == EMFILE || errno == ENFILE)
	{
		int			save_errno = errno;

		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("out of file descriptors: %m; release and retry")));
		errno = 0;
		if (ReleaseLruFile())
			goto TryAgain;
		errno = save_errno;
	}

	return NULL;
}

/*
 * Free an AllocateDesc of any type.
 *
 * The argument *must* point into the allocatedDescs[] array.
 */
static int
FreeDesc(AllocateDesc *desc)
{
	int			result;

	/* Close the underlying object */
	switch (desc->kind)
	{
		case AllocateDescFile:
			result = fclose(desc->desc.file);
			break;
		case AllocateDescPipe:
			result = pclose(desc->desc.file);
			break;
		case AllocateDescDir:
			result = closedir(desc->desc.dir);
			break;
		case AllocateDescRawFD:
			result = close(desc->desc.fd);
			break;
		default:
			elog(ERROR, "AllocateDesc kind not recognized");
			result = 0;			/* keep compiler quiet */
			break;
	}

	/* Compact storage in the allocatedDescs array */
	numAllocatedDescs--;
	*desc = allocatedDescs[numAllocatedDescs];

	return result;
}

/*
 * Close a file returned by AllocateFile.
 *
 * Note we do not check fclose's return value --- it is up to the caller
 * to handle close errors.
 */
int
FreeFile(FILE *file)
{
	int			i;

	DO_DB(elog(LOG, "FreeFile: Allocated %d", numAllocatedDescs));

	/* Remove file from list of allocated files, if it's present */
	for (i = numAllocatedDescs; --i >= 0;)
	{
		AllocateDesc *desc = &allocatedDescs[i];

		if (desc->kind == AllocateDescFile && desc->desc.file == file)
			return FreeDesc(desc);
	}

	/* Only get here if someone passes us a file not in allocatedDescs */
	elog(WARNING, "file passed to FreeFile was not obtained from AllocateFile");

	return fclose(file);
}

/*
 * Close a file returned by OpenTransientFile.
 *
 * Note we do not check close's return value --- it is up to the caller
 * to handle close errors.
 */
int
CloseTransientFile(int fd)
{
	int			i;

	DO_DB(elog(LOG, "CloseTransientFile: Allocated %d", numAllocatedDescs));

	/* Remove fd from list of allocated files, if it's present */
	for (i = numAllocatedDescs; --i >= 0;)
	{
		AllocateDesc *desc = &allocatedDescs[i];

		if (desc->kind == AllocateDescRawFD && desc->desc.fd == fd)
			return FreeDesc(desc);
	}

	/* Only get here if someone passes us a file not in allocatedDescs */
	elog(WARNING, "fd passed to CloseTransientFile was not obtained from OpenTransientFile");

	return close(fd);
}

/*
 * Routines that want to use <dirent.h> (ie, DIR*) should use AllocateDir
 * rather than plain opendir().  This lets fd.c deal with freeing FDs if
 * necessary to open the directory, and with closing it after an elog.
 * When done, call FreeDir rather than closedir.
 *
 * Ideally this should be the *only* direct call of opendir() in the backend.
 */
DIR *
AllocateDir(const char *dirname)
{
	DIR		   *dir;

	DO_DB(elog(LOG, "AllocateDir: Allocated %d (%s)",
			   numAllocatedDescs, dirname));

	/* Can we allocate another non-virtual FD? */
	if (!reserveAllocatedDesc())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("exceeded maxAllocatedDescs (%d) while trying to open directory \"%s\"",
						maxAllocatedDescs, dirname)));

	/* Close excess kernel FDs. */
	ReleaseLruFiles();

TryAgain:
	if ((dir = opendir(dirname)) != NULL)
	{
		AllocateDesc *desc = &allocatedDescs[numAllocatedDescs];

		desc->kind = AllocateDescDir;
		desc->desc.dir = dir;
		desc->create_subid = GetCurrentSubTransactionId();
		numAllocatedDescs++;
		return desc->desc.dir;
	}

	if (errno == EMFILE || errno == ENFILE)
	{
		int			save_errno = errno;

		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("out of file descriptors: %m; release and retry")));
		errno = 0;
		if (ReleaseLruFile())
			goto TryAgain;
		errno = save_errno;
	}

	return NULL;
}

/*
 * Read a directory opened with AllocateDir, ereport'ing any error.
 *
 * This is easier to use than raw readdir() since it takes care of some
 * otherwise rather tedious and error-prone manipulation of errno.  Also,
 * if you are happy with a generic error message for AllocateDir failure,
 * you can just do
 *
 *		dir = AllocateDir(path);
 *		while ((dirent = ReadDir(dir, path)) != NULL)
 *			process dirent;
 *		FreeDir(dir);
 *
 * since a NULL dir parameter is taken as indicating AllocateDir failed.
 * (Make sure errno hasn't been changed since AllocateDir if you use this
 * shortcut.)
 *
 * The pathname passed to AllocateDir must be passed to this routine too,
 * but it is only used for error reporting.
 */
struct dirent *
ReadDir(DIR *dir, const char *dirname)
{
	return ReadDirExtended(dir, dirname, ERROR);
}

/*
 * Alternate version that allows caller to specify the elevel for any
 * error report.  If elevel < ERROR, returns NULL on any error.
 */
static struct dirent *
ReadDirExtended(DIR *dir, const char *dirname, int elevel)
{
	struct dirent *dent;

	/* Give a generic message for AllocateDir failure, if caller didn't */
	if (dir == NULL)
	{
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m",
						dirname)));
		return NULL;
	}

	errno = 0;
	if ((dent = readdir(dir)) != NULL)
		return dent;

	if (errno)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not read directory \"%s\": %m",
						dirname)));
	return NULL;
}

/*
 * Close a directory opened with AllocateDir.
 *
 * Note we do not check closedir's return value --- it is up to the caller
 * to handle close errors.
 */
int
FreeDir(DIR *dir)
{
	int			i;

	DO_DB(elog(LOG, "FreeDir: Allocated %d", numAllocatedDescs));

	/* Remove dir from list of allocated dirs, if it's present */
	for (i = numAllocatedDescs; --i >= 0;)
	{
		AllocateDesc *desc = &allocatedDescs[i];

		if (desc->kind == AllocateDescDir && desc->desc.dir == dir)
			return FreeDesc(desc);
	}

	/* Only get here if someone passes us a dir not in allocatedDescs */
	elog(WARNING, "dir passed to FreeDir was not obtained from AllocateDir");

	return closedir(dir);
}


/*
 * Close a pipe stream returned by OpenPipeStream.
 */
int
ClosePipeStream(FILE *file)
{
	int			i;

	DO_DB(elog(LOG, "ClosePipeStream: Allocated %d", numAllocatedDescs));

	/* Remove file from list of allocated files, if it's present */
	for (i = numAllocatedDescs; --i >= 0;)
	{
		AllocateDesc *desc = &allocatedDescs[i];

		if (desc->kind == AllocateDescPipe && desc->desc.file == file)
			return FreeDesc(desc);
	}

	/* Only get here if someone passes us a file not in allocatedDescs */
	elog(WARNING, "file passed to ClosePipeStream was not obtained from OpenPipeStream");

	return pclose(file);
}

/*
 * closeAllVfds
 *
 * Force all VFDs into the physically-closed state, so that the fewest
 * possible number of kernel file descriptors are in use.  There is no
 * change in the logical state of the VFDs.
 */
void
closeAllVfds(void)
{
	Index		i;

	if (SizeVfdCache > 0)
	{
		Assert(FileIsNotOpen(0));		/* Make sure ring not corrupted */
		for (i = 1; i < SizeVfdCache; i++)
		{
			if (!FileIsNotOpen(i))
				LruDelete(i);
		}
	}
}


/*
 * SetTempTablespaces
 *
 * Define a list (actually an array) of OIDs of tablespaces to use for
 * temporary files.  This list will be used until end of transaction,
 * unless this function is called again before then.  It is caller's
 * responsibility that the passed-in array has adequate lifespan (typically
 * it'd be allocated in TopTransactionContext).
 */
void
SetTempTablespaces(Oid *tableSpaces, int numSpaces)
{
	Assert(numSpaces >= 0);
	tempTableSpaces = tableSpaces;
	numTempTableSpaces = numSpaces;

	/*
	 * Select a random starting point in the list.  This is to minimize
	 * conflicts between backends that are most likely sharing the same list
	 * of temp tablespaces.  Note that if we create multiple temp files in the
	 * same transaction, we'll advance circularly through the list --- this
	 * ensures that large temporary sort files are nicely spread across all
	 * available tablespaces.
	 */
	if (numSpaces > 1)
		nextTempTableSpace = random() % numSpaces;
	else
		nextTempTableSpace = 0;
}

/*
 * TempTablespacesAreSet
 *
 * Returns TRUE if SetTempTablespaces has been called in current transaction.
 * (This is just so that tablespaces.c doesn't need its own per-transaction
 * state.)
 */
bool
TempTablespacesAreSet(void)
{
	return (numTempTableSpaces >= 0);
}

/*
 * GetNextTempTableSpace
 *
 * Select the next temp tablespace to use.  A result of InvalidOid means
 * to use the current database's default tablespace.
 */
Oid
GetNextTempTableSpace(void)
{
	if (numTempTableSpaces > 0)
	{
		/* Advance nextTempTableSpace counter with wraparound */
		if (++nextTempTableSpace >= numTempTableSpaces)
			nextTempTableSpace = 0;
		return tempTableSpaces[nextTempTableSpace];
	}
	return InvalidOid;
}


/*
 * AtEOSubXact_Files
 *
 * Take care of subtransaction commit/abort.  At abort, we close temp files
 * that the subtransaction may have opened.  At commit, we reassign the
 * files that were opened to the parent subtransaction.
 */
void
AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid,
				  SubTransactionId parentSubid)
{
	Index		i;

	for (i = 0; i < numAllocatedDescs; i++)
	{
		if (allocatedDescs[i].create_subid == mySubid)
		{
			if (isCommit)
				allocatedDescs[i].create_subid = parentSubid;
			else
			{
				/* have to recheck the item after FreeDesc (ugly) */
				FreeDesc(&allocatedDescs[i--]);
			}
		}
	}
}

/*
 * AtEOXact_Files
 *
 * This routine is called during transaction commit or abort (it doesn't
 * particularly care which).  All still-open per-transaction temporary file
 * VFDs are closed, which also causes the underlying files to be deleted
 * (although they should've been closed already by the ResourceOwner
 * cleanup). Furthermore, all "allocated" stdio files are closed. We also
 * forget any transaction-local temp tablespace list.
 */
void
AtEOXact_Files(void)
{
	CleanupTempFiles(false);
	tempTableSpaces = NULL;
	numTempTableSpaces = -1;
}

/*
 * AtProcExit_Files
 *
 * on_proc_exit hook to clean up temp files during backend shutdown.
 * Here, we want to clean up *all* temp files including interXact ones.
 */
static void
AtProcExit_Files(int code, Datum arg)
{
	CleanupTempFiles(true);
}

/*
 * Close temporary files and delete their underlying files.
 *
 * isProcExit: if true, this is being called as the backend process is
 * exiting. If that's the case, we should remove all temporary files; if
 * that's not the case, we are being called for transaction commit/abort
 * and should only remove transaction-local temp files.  In either case,
 * also clean up "allocated" stdio files, dirs and fds.
 */
static void
CleanupTempFiles(bool isProcExit)
{
	Index		i;

	/*
	 * Careful here: at proc_exit we need extra cleanup, not just
	 * xact_temporary files.
	 */
	if (isProcExit || have_xact_temporary_files)
	{
		Assert(FileIsNotOpen(0));		/* Make sure ring not corrupted */
		for (i = 1; i < SizeVfdCache; i++)
		{
			unsigned short fdstate = VfdCache[i].fdstate;

			if ((fdstate & FD_TEMPORARY) && VfdCache[i].fileName != NULL)
			{
				/*
				 * If we're in the process of exiting a backend process, close
				 * all temporary files. Otherwise, only close temporary files
				 * local to the current transaction. They should be closed by
				 * the ResourceOwner mechanism already, so this is just a
				 * debugging cross-check.
				 */
				if (isProcExit)
					FileClose(i);
				else if (fdstate & FD_XACT_TEMPORARY)
				{
					elog(WARNING,
						 "temporary file %s not closed at end-of-transaction",
						 VfdCache[i].fileName);
					FileClose(i);
				}
			}
		}

		have_xact_temporary_files = false;
	}

	/* Clean up "allocated" stdio files, dirs and fds. */
	while (numAllocatedDescs > 0)
		FreeDesc(&allocatedDescs[0]);
}


/*
 * Remove temporary and temporary relation files left over from a prior
 * postmaster session
 *
 * This should be called during postmaster startup.  It will forcibly
 * remove any leftover files created by OpenTemporaryFile and any leftover
 * temporary relation files created by mdcreate.
 *
 * NOTE: we could, but don't, call this during a post-backend-crash restart
 * cycle.  The argument for not doing it is that someone might want to examine
 * the temp files for debugging purposes.  This does however mean that
 * OpenTemporaryFile had better allow for collision with an existing temp
 * file name.
 */
void
RemovePgTempFiles(void)
{
	char		temp_path[MAXPGPATH];
	DIR		   *spc_dir;
	struct dirent *spc_de;

	/*
	 * First process temp files in pg_default ($PGDATA/base)
	 */
	snprintf(temp_path, sizeof(temp_path), "base/%s", PG_TEMP_FILES_DIR);
	RemovePgTempFilesInDir(temp_path);
	RemovePgTempRelationFiles("base");

	/*
	 * Cycle through temp directories for all non-default tablespaces.
	 */
	spc_dir = AllocateDir("pg_tblspc");

	while ((spc_de = ReadDir(spc_dir, "pg_tblspc")) != NULL)
	{
		if (strcmp(spc_de->d_name, ".") == 0 ||
			strcmp(spc_de->d_name, "..") == 0)
			continue;

		snprintf(temp_path, sizeof(temp_path), "pg_tblspc/%s/%s/%s",
			spc_de->d_name, TABLESPACE_VERSION_DIRECTORY, PG_TEMP_FILES_DIR);
		RemovePgTempFilesInDir(temp_path);

		snprintf(temp_path, sizeof(temp_path), "pg_tblspc/%s/%s",
				 spc_de->d_name, TABLESPACE_VERSION_DIRECTORY);
		RemovePgTempRelationFiles(temp_path);
	}

	FreeDir(spc_dir);

	/*
	 * In EXEC_BACKEND case there is a pgsql_tmp directory at the top level of
	 * DataDir as well.
	 */
#ifdef EXEC_BACKEND
	RemovePgTempFilesInDir(PG_TEMP_FILES_DIR);
#endif
}

/* Process one pgsql_tmp directory for RemovePgTempFiles */
static void
RemovePgTempFilesInDir(const char *tmpdirname)
{
	DIR		   *temp_dir;
	struct dirent *temp_de;
	char		rm_path[MAXPGPATH];

	temp_dir = AllocateDir(tmpdirname);
	if (temp_dir == NULL)
	{
		/* anything except ENOENT is fishy */
		if (errno != ENOENT)
			elog(LOG,
				 "could not open temporary-files directory \"%s\": %m",
				 tmpdirname);
		return;
	}

	while ((temp_de = ReadDir(temp_dir, tmpdirname)) != NULL)
	{
		if (strcmp(temp_de->d_name, ".") == 0 ||
			strcmp(temp_de->d_name, "..") == 0)
			continue;

		snprintf(rm_path, sizeof(rm_path), "%s/%s",
				 tmpdirname, temp_de->d_name);

		if (strncmp(temp_de->d_name,
					PG_TEMP_FILE_PREFIX,
					strlen(PG_TEMP_FILE_PREFIX)) == 0)
			unlink(rm_path);	/* note we ignore any error */
		else
			elog(LOG,
				 "unexpected file found in temporary-files directory: \"%s\"",
				 rm_path);
	}

	FreeDir(temp_dir);
}

/* Process one tablespace directory, look for per-DB subdirectories */
static void
RemovePgTempRelationFiles(const char *tsdirname)
{
	DIR		   *ts_dir;
	struct dirent *de;
	char		dbspace_path[MAXPGPATH];

	ts_dir = AllocateDir(tsdirname);
	if (ts_dir == NULL)
	{
		/* anything except ENOENT is fishy */
		if (errno != ENOENT)
			elog(LOG,
				 "could not open tablespace directory \"%s\": %m",
				 tsdirname);
		return;
	}

	while ((de = ReadDir(ts_dir, tsdirname)) != NULL)
	{
		int			i = 0;

		/*
		 * We're only interested in the per-database directories, which have
		 * numeric names.  Note that this code will also (properly) ignore "."
		 * and "..".
		 */
		while (isdigit((unsigned char) de->d_name[i]))
			++i;
		if (de->d_name[i] != '\0' || i == 0)
			continue;

		snprintf(dbspace_path, sizeof(dbspace_path), "%s/%s",
				 tsdirname, de->d_name);
		RemovePgTempRelationFilesInDbspace(dbspace_path);
	}

	FreeDir(ts_dir);
}

/* Process one per-dbspace directory for RemovePgTempRelationFiles */
static void
RemovePgTempRelationFilesInDbspace(const char *dbspacedirname)
{
	DIR		   *dbspace_dir;
	struct dirent *de;
	char		rm_path[MAXPGPATH];

	dbspace_dir = AllocateDir(dbspacedirname);
	if (dbspace_dir == NULL)
	{
		/* we just saw this directory, so it really ought to be there */
		elog(LOG,
			 "could not open dbspace directory \"%s\": %m",
			 dbspacedirname);
		return;
	}

	while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL)
	{
		if (!looks_like_temp_rel_name(de->d_name))
			continue;

		snprintf(rm_path, sizeof(rm_path), "%s/%s",
				 dbspacedirname, de->d_name);

		unlink(rm_path);		/* note we ignore any error */
	}

	FreeDir(dbspace_dir);
}

/* t<digits>_<digits>, or t<digits>_<digits>_<forkname> */
static bool
looks_like_temp_rel_name(const char *name)
{
	int			pos;
	int			savepos;

	/* Must start with "t". */
	if (name[0] != 't')
		return false;

	/* Followed by a non-empty string of digits and then an underscore. */
	for (pos = 1; isdigit((unsigned char) name[pos]); ++pos)
		;
	if (pos == 1 || name[pos] != '_')
		return false;

	/* Followed by another nonempty string of digits. */
	for (savepos = ++pos; isdigit((unsigned char) name[pos]); ++pos)
		;
	if (savepos == pos)
		return false;

	/* We might have _forkname or .segment or both. */
	if (name[pos] == '_')
	{
		int			forkchar = forkname_chars(&name[pos + 1], NULL);

		if (forkchar <= 0)
			return false;
		pos += forkchar + 1;
	}
	if (name[pos] == '.')
	{
		int			segchar;

		for (segchar = 1; isdigit((unsigned char) name[pos + segchar]); ++segchar)
			;
		if (segchar <= 1)
			return false;
		pos += segchar;
	}

	/* Now we should be at the end. */
	if (name[pos] != '\0')
		return false;
	return true;
}


/*
 * Issue fsync recursively on PGDATA and all its contents.
 *
 * We fsync regular files and directories wherever they are, but we
 * follow symlinks only for pg_xlog and immediately under pg_tblspc.
 * Other symlinks are presumed to point at files we're not responsible
 * for fsyncing, and might not have privileges to write at all.
 *
 * Errors are logged but not considered fatal; that's because this is used
 * only during database startup, to deal with the possibility that there are
 * issued-but-unsynced writes pending against the data directory.  We want to
 * ensure that such writes reach disk before anything that's done in the new
 * run.  However, aborting on error would result in failure to start for
 * harmless cases such as read-only files in the data directory, and that's
 * not good either.
 *
 * Note we assume we're chdir'd into PGDATA to begin with.
 */
void
SyncDataDirectory(void)
{
	bool		xlog_is_symlink;

	/* We can skip this whole thing if fsync is disabled. */
	if (!enableFsync)
		return;

	/*
	 * If pg_xlog is a symlink, we'll need to recurse into it separately,
	 * because the first walkdir below will ignore it.
	 */
	xlog_is_symlink = false;

#ifndef WIN32
	{
		struct stat st;

		if (lstat("pg_xlog", &st) < 0)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							"pg_xlog")));
		else if (S_ISLNK(st.st_mode))
			xlog_is_symlink = true;
	}
#else
	if (pgwin32_is_junction("pg_xlog"))
		xlog_is_symlink = true;
#endif

	/*
	 * If possible, hint to the kernel that we're soon going to fsync the data
	 * directory and its contents.  Errors in this step are even less
	 * interesting than normal, so log them only at DEBUG1.
	 */
#ifdef PG_FLUSH_DATA_WORKS
	walkdir(".", pre_sync_fname, false, DEBUG1);
	if (xlog_is_symlink)
		walkdir("pg_xlog", pre_sync_fname, false, DEBUG1);
	walkdir("pg_tblspc", pre_sync_fname, true, DEBUG1);
#endif

	/*
	 * Now we do the fsync()s in the same order.
	 *
	 * The main call ignores symlinks, so in addition to specially processing
	 * pg_xlog if it's a symlink, pg_tblspc has to be visited separately with
	 * process_symlinks = true.  Note that if there are any plain directories
	 * in pg_tblspc, they'll get fsync'd twice.  That's not an expected case
	 * so we don't worry about optimizing it.
	 */
	walkdir(".", fsync_fname_ext, false, LOG);
	if (xlog_is_symlink)
		walkdir("pg_xlog", fsync_fname_ext, false, LOG);
	walkdir("pg_tblspc", fsync_fname_ext, true, LOG);
}

/*
 * walkdir: recursively walk a directory, applying the action to each
 * regular file and directory (including the named directory itself).
 *
 * If process_symlinks is true, the action and recursion are also applied
 * to regular files and directories that are pointed to by symlinks in the
 * given directory; otherwise symlinks are ignored.  Symlinks are always
 * ignored in subdirectories, ie we intentionally don't pass down the
 * process_symlinks flag to recursive calls.
 *
 * Errors are reported at level elevel, which might be ERROR or less.
 *
 * See also walkdir in initdb.c, which is a frontend version of this logic.
 */
static void
walkdir(const char *path,
		void (*action) (const char *fname, bool isdir, int elevel),
		bool process_symlinks,
		int elevel)
{
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir(path);
	if (dir == NULL)
	{
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", path)));
		return;
	}

	while ((de = ReadDirExtended(dir, path, elevel)) != NULL)
	{
		char		subpath[MAXPGPATH];
		struct stat fst;
		int			sret;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(subpath, MAXPGPATH, "%s/%s", path, de->d_name);

		if (process_symlinks)
			sret = stat(subpath, &fst);
		else
			sret = lstat(subpath, &fst);

		if (sret < 0)
		{
			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", subpath)));
			continue;
		}

		if (S_ISREG(fst.st_mode))
			(*action) (subpath, false, elevel);
		else if (S_ISDIR(fst.st_mode))
			walkdir(subpath, action, false, elevel);
	}

	FreeDir(dir);				/* we ignore any error here */

	/*
	 * It's important to fsync the destination directory itself as individual
	 * file fsyncs don't guarantee that the directory entry for the file is
	 * synced.
	 */
	(*action) (path, true, elevel);
}


/*
 * Hint to the OS that it should get ready to fsync() this file.
 *
 * Ignores errors trying to open unreadable files, and logs other errors at a
 * caller-specified level.
 */
#ifdef PG_FLUSH_DATA_WORKS

static void
pre_sync_fname(const char *fname, bool isdir, int elevel)
{
	int			fd;

	fd = OpenTransientFile((char *) fname, O_RDONLY | PG_BINARY, 0);

	if (fd < 0)
	{
		if (errno == EACCES || (isdir && errno == EISDIR))
			return;
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", fname)));
		return;
	}

	/*
	 * We ignore errors from pg_flush_data() because this is only a hint.
	 */
	(void) pg_flush_data(fd, 0, 0);

	(void) CloseTransientFile(fd);
}

#endif   /* PG_FLUSH_DATA_WORKS */

/*
 * fsync_fname_ext -- Try to fsync a file or directory
 *
 * Ignores errors trying to open unreadable files, or trying to fsync
 * directories on systems where that isn't allowed/required, and logs other
 * errors at a caller-specified level.
 */
static void
fsync_fname_ext(const char *fname, bool isdir, int elevel)
{
	int			fd;
	int			flags;
	int			returncode;

	/*
	 * Some OSs require directories to be opened read-only whereas other
	 * systems don't allow us to fsync files opened read-only; so we need both
	 * cases here.  Using O_RDWR will cause us to fail to fsync files that are
	 * not writable by our userid, but we assume that's OK.
	 */
	flags = PG_BINARY;
	if (!isdir)
		flags |= O_RDWR;
	else
		flags |= O_RDONLY;

	/*
	 * Open the file, silently ignoring errors about unreadable files (or
	 * unsupported operations, e.g. opening a directory under Windows), and
	 * logging others.
	 */
	fd = OpenTransientFile((char *) fname, flags, 0);
	if (fd < 0)
	{
		if (errno == EACCES || (isdir && errno == EISDIR))
			return;
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", fname)));
		return;
	}

	returncode = pg_fsync(fd);

	/*
	 * Some OSes don't allow us to fsync directories at all, so we can ignore
	 * those errors. Anything else needs to be logged.
	 */
	if (returncode != 0 && !(isdir && errno == EBADF))
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", fname)));

	(void) CloseTransientFile(fd);
}

static int getIdxFromSeekpos(File file)
{
    off_t low_idx, mid_idx, high_idx, range;
    
    low_idx = 0;
    high_idx = VfdCache[file].pm_list_idx;
    range = high_idx - low_idx;

    while(range > 0) {
        mid_idx = (high_idx + low_idx) / 2;
        
        if (VfdCache[file].seekPos < VfdCache[file].pm_size_list[mid_idx]) {
            high_idx = mid_idx;
        } else {
            low_idx = mid_idx + 1;
        }

        range = high_idx - low_idx;
    }
    
    return range;
}

