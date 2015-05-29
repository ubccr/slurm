# Makefile for job_submit/save_script plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = job_submit_ccr_qos.la

# Job submit save_script plugin.
job_submit_ccr_qos_la_SOURCES = job_submit_ccr_qos.c
job_submit_ccr_qos_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)
