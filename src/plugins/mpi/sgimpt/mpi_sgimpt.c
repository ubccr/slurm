/*****************************************************************************\
 **  mpi_sgimpt.c - Library routines for initiating jobs with SGI MPT support
 **  $Id$
 *****************************************************************************
 *  Copyright (C) 2013 SGI International, Inc.  All rights reserved.
 *  Copyright (C) 2004 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://www.schedmd.com/slurmdocs/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#if     HAVE_CONFIG_H
#  include "config.h"
#endif

#include <dlfcn.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/types.h>

#include "slurm/slurm_errno.h"

#include "src/common/slurm_xlator.h"
#include "src/common/mpi.h"
#include "src/common/env.h"
#include "src/slurmd/slurmstepd/slurmstepd_job.h"

/*
 * These variables are required by the generic plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - a string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - a string suggesting the type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  SLURM uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *      <application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "switch" for SLURM switch) and <method> is a description
 * of how this plugin satisfies that application.  SLURM will only load
 * a switch plugin if the plugin_type string has a prefix of "switch/".
 *
 * plugin_version - an unsigned 32-bit integer giving the version number
 * of the plugin.  If major and minor revisions are desired, the major
 * version number may be multiplied by a suitable magnitude constant such
 * as 100 or 1000.  Various SLURM versions will likely require a certain
 * minimum version for their plugins as this API matures.
 */
const char plugin_name[]        = "mpi sgimpt plugin";
const char plugin_type[]        = "mpi/sgimpt";
const uint32_t plugin_version   = 100;

static pthread_t mpt_thread;
static void * mpt_xlib;
static uint32_t mpt_secret;
static int mpt_listen_sock;

static int    (*MPI_RM2_init_p)(char **);
static void * (*MPI_RM2_handle_p)(void);
static int    (*MPI_RM2_sethosts_p)(void *, int, char **, const uint16_t *);
static int    (*MPI_RM2_start_p)(void *, int, uint32_t);
static int    (*MPI_RM2_monitor_p)(void *);
static int    (*MPI_RM2_finalize_p)(void *);

#define LOOKUP_SYM(x) do { \
		x ##_p = dlsym(mpt_xlib, #x); \
		if (NULL == x ## _p) {return -1;} \
	} while(0)

int p_mpi_hook_slurmstepd_prefork(const slurmd_job_t *job, char ***env)
{
	debug("mpi/sgimpt: slurmstepd prefork");

	/* Tell MPT which shepherd in our world we are */
	env_array_overwrite_fmt(env, "MPI_DRANK", "%d", job->nodeid);

	return SLURM_SUCCESS;
}

int p_mpi_hook_slurmstepd_task(const mpi_plugin_task_info_t*job,
			       char ***env)
{
	char * ipaddr = getenvp(*env, "SLURM_LAUNCH_NODE_IPADDR");
	struct in_addr addr;

	debug("Using mpi/sgimpt");

	inet_pton(AF_INET, ipaddr, &addr);

	/* Let the shepherds know how to contact libxmpi */
	env_array_overwrite_fmt(env, "MPI_ENVIRONMENT", "%x %s 0 %s 0",
				   addr.s_addr,
				   getenvp(*env, "SLURM_SGIMPT_PORT"),
				   getenvp(*env, "SLURM_SGIMPT_SECRET"));

	return SLURM_SUCCESS;
}

static int
load_libs(void)
{
	mpt_xlib = dlopen("libxmpi.so", RTLD_LAZY | RTLD_LOCAL);
	if (!mpt_xlib) {-1;}

	LOOKUP_SYM(MPI_RM2_init);
	LOOKUP_SYM(MPI_RM2_handle);
	LOOKUP_SYM(MPI_RM2_sethosts);
	LOOKUP_SYM(MPI_RM2_start);
	LOOKUP_SYM(MPI_RM2_monitor);
	LOOKUP_SYM(MPI_RM2_finalize);

	return 0;
}

/*
 * MPT uses a random private 32-bit value to provide weak authentication of
 * connections
 */
static uint32_t
init_secret(void)
{
	uint32_t secret = 0xF0F00F0F;
	struct timeval tv;
	int fd;

	if ((fd = open("/dev/urandom", O_RDONLY)) >= 0) {
		read(fd, &secret, sizeof(uint32_t));
		close(fd);
	} else {
		gettimeofday(&tv, NULL);
		secret = tv.tv_usec;
	}

	return secret;
}

static void *
mpt_func(void * arg)
{
	const mpi_plugin_client_info_t * job =
		(const mpi_plugin_client_info_t *)arg;
	void * handle;
	int rc, i;
	int nhosts = job->step_layout->node_cnt;
	uint16_t * nprocs = job->step_layout->tasks;
	char ** hnames;
	hostlist_t hl;

	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

	/* Get a handle to this MPI world */
	handle = MPI_RM2_handle_p();
	if (!handle) {goto error;}

	hl = hostlist_create(job->step_layout->node_list);
	hnames = (char**)xmalloc(nhosts * sizeof(char*));
	if (!hl || !hnames) {
		error("Out of memory");
		return NULL;
	}

	for (i = 0; i < nhosts; i++) {hnames[i] = hostlist_nth(hl, i);}

	/* Let MPT know the hosts and tasks per node */
	rc = MPI_RM2_sethosts_p(handle, nhosts, hnames, nprocs);
	if (rc) {goto error;}

	for (i = 0; i < nhosts; i++) {free(hnames[i]);}
	xfree(hnames);
	hostlist_destroy(hl);

	/* Wait for the launch to complete */
	rc = MPI_RM2_start_p(handle, mpt_listen_sock, mpt_secret);
	if (rc) {goto error;}

	/* Let the jobs get going and wait for them to complete */
	rc = MPI_RM2_monitor_p(handle);
	if (rc) {goto error;}

	/* Clean things up */
	rc = MPI_RM2_finalize_p(handle);
	if (rc) {goto error;}

	return NULL;
error:
	error("Error with interacting with MPT\n");
	return NULL;
}

mpi_plugin_client_state_t *
p_mpi_hook_client_prelaunch(const mpi_plugin_client_info_t *job, char ***env)
{
	char hname[256];
	struct sockaddr_in sin;
	socklen_t sinlen = sizeof(sin);

	debug("Using mpi/sgimpt");

	if (load_libs()) {
		error("Could not load MPT's libxmpi.so\n");
		return NULL;
	}

	mpt_secret = init_secret();

	/* For listening for MPT shepherds on */
	if (0 > (mpt_listen_sock = socket(AF_INET, SOCK_STREAM, 0))) {
		error("socket: %m");
		return NULL;
	}

	memset(&sin, 0, sinlen);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
	sin.sin_port = htons(0);

	if (0 > bind(mpt_listen_sock, (struct sockaddr*)&sin, sinlen)) {
		error("bind: %m");
		return NULL;
	}
	if (0 > listen(mpt_listen_sock, 256)) {
		error("listen: %m");
		return NULL;
	}
	if (0 > getsockname(mpt_listen_sock, (void*)&sin, &sinlen)) {
		error("getsockname: %m");
		return NULL;
	}

	env_array_overwrite_fmt(env, "SLURM_SGIMPT_SECRET", "%x", mpt_secret);
	env_array_overwrite_fmt(env, "SLURM_SGIMPT_PORT", "%hu",
				   ntohs(sin.sin_port));

	/* Get the global services up and going */
	if (MPI_RM2_init_p(*env)) {return NULL;}

	/* Provide MPT services in a different thread */
	if (pthread_create(&mpt_thread, NULL, mpt_func, (void*)job)) {
		error("pthread_create: %m");
		return NULL;
	}

	/* only return NULL on error */
	return (void *)0xdeadbeef;
}

int p_mpi_hook_client_single_task_per_node(void)
{
	return false;
}

int p_mpi_hook_client_fini(mpi_plugin_client_state_t *state)
{
	int * ret;

	pthread_cancel(mpt_thread);
	if (pthread_join(mpt_thread, (void**)&ret)) {
		error("pthread_join: %m");
		return SLURM_ERROR;
	}

	return SLURM_SUCCESS;
}
