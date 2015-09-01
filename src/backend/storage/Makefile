#
# Makefile for the storage manager subsystem
#
# src/backend/storage/Makefile
#

subdir = src/backend/storage
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(STORAGE),MMFILE)
       SUBDIRS     = buffer mmfile freespace ipc large_object lmgr page smgr
else
       SUBDIRS     = buffer file freespace ipc large_object lmgr page smgr
endif

include $(top_srcdir)/src/backend/common.mk
