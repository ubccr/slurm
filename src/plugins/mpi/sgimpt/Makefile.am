# Makefile for mpi/sgimpt plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

INCLUDES = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = mpi_sgimpt.la

# SGI MPT MPI plugin.
mpi_sgimpt_la_SOURCES = mpi_sgimpt.c $(top_srcdir)/src/common/mpi.h 
mpi_sgimpt_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)
