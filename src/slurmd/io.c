/*****************************************************************************\
 * src/slurmd/io.c - I/O handling routines for slurmd
 * $Id$
 *****************************************************************************
 *  Copyright (C) 2002 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Mark Grondona <mgrondona@llnl.gov>.
 *  UCRL-CODE-2002-040.
 *  
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://www.llnl.gov/linux/slurm/>.
 *  
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *  
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *  
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.
\*****************************************************************************/

#if HAVE_CONFIG_H
#  include <config.h>
#endif

#if HAVE_UNISTD_H
#  include <unistd.h>
#endif

#if HAVE_STRING_H
#  include <string.h>
#endif

#if HAVE_STDLIB_H
#  include <stdlib.h>
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include <src/common/eio.h>
#include <src/common/cbuf.h>
#include <src/common/log.h>
#include <src/common/fd.h>
#include <src/common/list.h>
#include <src/common/xmalloc.h>
#include <src/common/xsignal.h>

#include <src/slurmd/job.h>
#include <src/slurmd/shm.h>
#include <src/slurmd/io.h>

typedef enum slurmd_io_tupe {
	TASK_STDERR = 0,
	TASK_STDOUT,
	TASK_STDIN,
	CLIENT_STDERR,
	CLIENT_STDOUT,
	CLIENT_STDIN
} slurmd_io_type_t;

static char *_io_str[] = 
{
	"task stderr",
	"task stdout",
	"task stdin",
	"client stderr",
	"client stdout",
	"client stdin"
};

/* The IO information structure
 */
struct io_info {
#ifndef NDEBUG
#define IO_MAGIC  0x10101
	int              magic;
#endif
	uint32_t         id;             /* global task id             */
	io_obj_t        *obj;            /* pointer back to eio object */
	slurmd_job_t    *job;		 /* pointer back to job data   */
	task_info_t     *task;           /* pointer back to task data  */
	cbuf_t           buf;		 /* IO buffer	               */
	List             readers;        /* list of current readers    */
	List             writers;        /* list of current writers    */
	slurmd_io_type_t type;           /* type of IO object          */
	unsigned         eof:1;          /* obj recvd or generated EOF */

	unsigned         disconnected:1; /* signifies that fd is not 
					  * connected to anything
					  * (e.g. A "ghost" client attached
					  * to a task.)
					  */
};


static int    _io_init_pipes(task_info_t *t);
static void   _io_prepare_clients(slurmd_job_t *);
static void   _io_prepare_tasks(slurmd_job_t *);
static void   _io_prepare_files(slurmd_job_t *);
static void * _io_thr(void *);
static int    _io_write_header(struct io_info *, srun_info_t *);
static void   _io_connect_objs(io_obj_t *, io_obj_t *);
static int    _validate_io_list(List objList);
static int    _shutdown_task_obj(struct io_info *t);
static int    find_obj(void *obj, void *key);
static int    find_fd(void *obj, void *key);
static bool   _isa_client(struct io_info *io);
static bool   _isa_task(struct io_info *io);
static void   _io_client_attach(io_obj_t *, io_obj_t *, io_obj_t *, 
		                List objList);

static struct io_obj  * _io_obj_create(int fd, void *arg);
static struct io_info * _io_info_create(uint32_t id);
static struct io_obj  * _io_obj(slurmd_job_t *, int, uint32_t, int);
static void           * _io_thr(void *arg);


static struct io_operations * _ops_copy(struct io_operations *ops);
static void                   _ops_destroy(struct io_operations *ops);

/* Slurmd I/O objects:
 * N   task   stderr, stdout objs (read-only)
 * N*M client stderr, stdout objs (read-write) (possibly a file)
 * N   task   stdin          objs (write only) (possibly a file)
 */

static bool _readable(io_obj_t *);
static bool _writable(io_obj_t *);
static int  _write(io_obj_t *, List);
static int  _task_read(io_obj_t *, List);
static int  _client_read(io_obj_t *, List);
static int  _task_error(io_obj_t *, List);
static int  _client_error(io_obj_t *, List);
static int  _connecting_write(io_obj_t *, List);

/* Task Output operations (TASK_STDOUT, TASK_STDERR)
 * These objects are never writable --
 * therefore no need for writeable and handle_write methods
 */
struct io_operations task_out_ops = {
        readable:	&_readable,
	handle_read:	&_task_read,
        handle_error:	&_task_error
};

/* Task Input operations (TASK_STDIN)
 * input objects are never readable
 */
struct io_operations task_in_ops = {
	writable:	&_writable,
	handle_write:	&_write,
	handle_error:	&_task_error,
};
			
/* Normal client operations (CLIENT_STDOUT, CLIENT_STDERR, CLIENT_STDIN)
 * these methods apply to clients which are considered
 * "connected" i.e. in the case of srun, they've read
 * the so-called IO-header data
 */
struct io_operations client_ops = {
        readable:	&_readable,
	writable:	&_writable,
	handle_read:	&_client_read,
	handle_write:	&_write,
	handle_error:	&_client_error,
};


/* Connecting client operations --
 * clients use a connecting write until they've
 * written out the IO header data. Not until that
 * point will clients be able to read regular 
 * stdout/err data, so we treat them special
 */
struct io_operations connecting_client_ops = {
        writable:	&_writable,
        handle_write:	&_connecting_write,
        handle_error:   &_client_error
};

/* 
 * Empty SIGHUP handler used to interrupt EIO thread 
 * system calls
 */
static void
_hup_handler(int sig) {;}

int
io_spawn_handler(slurmd_job_t *job) 
{
	pthread_attr_t attr;

	xsignal(SIGHUP, &_hup_handler);
	
	if (io_init_pipes(job) == SLURM_FAILURE) {
		error("io_handler: init_pipes failed: %m");
		return SLURM_FAILURE;
	}

	/* create task IO objects and append these to the objs list
	 *
	 * XXX check for errors?
	 */
	_io_prepare_tasks(job);

	if ((errno = pthread_attr_init(&attr)) != 0)
		error("pthread_attr_init: %m");

#ifdef PTHREAD_SCOPE_SYSTEM
	if ((errno = pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM)) != 0)
		error("pthread_attr_setscope: %m");
#endif 
	xassert(_validate_io_list(job->objs));

	pthread_create(&job->ioid, &attr, &_io_thr, (void *)job);

	/* open 2*ntask initial connections or files for stdout/err 
	 * append these to objs list 
	 */
	if (list_count(job->sruns) > 0)
		_io_prepare_clients(job);
	_io_prepare_files(job);

	return 0;
}

static int
_xclose(int fd)
{
	int rc;
	do rc = close(fd);
	while (rc == -1 && errno == EINTR);
	return rc;
}

/* Close child fds in parent */
static void
_io_finalize(task_info_t *t)
{
	if (_xclose(t->pin[0] ) < 0)
		error("close(stdin) : %m");
	if (_xclose(t->pout[1]) < 0)
		error("close(stdout): %m");
	if (_xclose(t->perr[1]) < 0)
		error("close(stderr): %m");
}

void 
io_close_all(slurmd_job_t *job)
{
	int i;
	for (i = 0; i < job->ntasks; i++)
		_io_finalize(job->task[i]);
}

static void
_handle_unprocessed_output(slurmd_job_t *job)
{
	int i;
	/* XXX Do something with unwritten IO */
	for (i = 0; i < job->ntasks; i++) {
		size_t       n;
		task_info_t *t = job->task[i];
		List  readers = ((struct io_info *)t->out->arg)->readers;
		struct io_info *io = list_peek(readers);
		if (io->buf && (n = cbuf_used(io->buf)))
			job_error(job, "%ld bytes of stdout unprocessed", n);
		readers = ((struct io_info *)t->err->arg)->readers;
		io = list_peek(readers);
		if (io->buf && (n = cbuf_used(io->buf)))
			job_error(job, "%ld bytes of stderr unprocessed", n);
	}
}

static void *
_io_thr(void *arg)
{
	slurmd_job_t *job = (slurmd_job_t *) arg;
	io_handle_events(job->objs);
	debug("IO handler exited");
	_handle_unprocessed_output(job);
	return (void *)1;
}

static void
_io_prepare_tasks(slurmd_job_t *job)
{
	int          i;
	task_info_t *t;
	io_obj_t    *obj;

	for (i = 0; i < job->ntasks; i++) {
		t = job->task[i];

		t->in  = _io_obj(job, t->pin[1],  t->gid, TASK_STDIN );
		list_append(job->objs, (void *)t->in );

		t->out = _io_obj(job, t->pout[0], t->gid, TASK_STDOUT);
		list_append(job->objs, (void *)t->out);

		/* "ghost" stdout client buffers task data without sending 
		 * it anywhere
		 */
		obj    = _io_obj(job, -1, t->gid, CLIENT_STDOUT);
		_io_client_attach(obj, t->out, NULL, job->objs);

		t->err = _io_obj(job, t->perr[0], t->gid, TASK_STDERR);
		list_append(job->objs, (void *)t->err);

		/* "fake" stderr client buffers task data without sending 
		 * it anywhere
		 */
		obj    = _io_obj(job, -1, t->gid, CLIENT_STDERR);
		_io_client_attach(obj, t->err, NULL, job->objs);
	}

	xassert(_validate_io_list(job->objs));
}

/*
 * Turn off obj's readable() function such that it is never
 * checked for readability
 */
static inline void
_obj_set_unreadable(io_obj_t *obj)
{
	obj->ops->readable = NULL;
}

static inline void
_obj_set_unwritable(io_obj_t *obj)
{
	obj->ops->writable = NULL;
}

static void
_io_add_connecting(slurmd_job_t *job, task_info_t *t, srun_info_t *srun, 
		   slurmd_io_type_t type)
{
	io_obj_t *obj  = NULL;
	int       sock = -1;

	if ((sock = (int) slurm_open_stream(&srun->ioaddr)) < 0) {
		error("connect io: %m");
		/* XXX retry or silently fail? 
		 *     fail for now.
		 */
		return;
	}
		
	fd_set_nonblocking(sock);
	fd_set_close_on_exec(sock);

	obj      = _io_obj(job, sock, t->gid, type);
	obj->ops = &connecting_client_ops;
	_io_write_header(obj->arg, srun);
	list_append(job->objs, (void *)obj);
}

/* 
 * create initial client objs for N tasks
 */
static void
_io_prepare_clients(slurmd_job_t *job)
{
	int          i;
	srun_info_t *srun;

	xassert(list_count(job->sruns) == 1);

	srun = list_peek(job->sruns);
	if (srun->noconnect)
		return;

	/* 
	 * connect back to clients for stdin/out/err
	 */
	for (i = 0; i < job->ntasks; i++) {
		_io_add_connecting(job, job->task[i], srun, CLIENT_STDOUT); 
		_io_add_connecting(job, job->task[i], srun, CLIENT_STDERR); 

		/* kick IO thread */
		pthread_kill(job->ioid, SIGHUP);
	}
}

static int
_open_task_file(char *filename, int flags)
{
	int fd;
	if (filename == NULL)
		return -1;
	if ((fd = open(filename, flags, 0644))< 0) {
		error ("Unable to open `%s': %m", filename);
		return -1;
	}
	fd_set_nonblocking(fd);
	fd_set_close_on_exec(fd);
	return fd;
}

static int
_open_output_file(slurmd_job_t *job, task_info_t *t, slurmd_io_type_t type)
{
	int       fd    = -1;
	io_obj_t *obj   = NULL;
	int       flags = O_CREAT|O_TRUNC|O_APPEND|O_WRONLY;
	char     *fname;

	xassert((type == CLIENT_STDOUT) || (type == CLIENT_STDERR));
	fname = (type == CLIENT_STDOUT) ? t->ofname : t->efname;

	if ((fd = _open_task_file(fname, flags)) > 0) {
		verbose("opened `%s' for %s fd %d", fname, _io_str[type], fd);
		obj  = _io_obj(job, fd, t->gid, type);
		_obj_set_unreadable(obj);
		xassert(obj->ops->writable != NULL);
		if (type == CLIENT_STDOUT)
			_io_client_attach(obj, t->out, NULL, job->objs);
		else
			_io_client_attach(obj, t->err, NULL, job->objs);
	} else
		error("Unable to open `%s': %m", fname);

	_validate_io_list(job->objs);

	return fd;
}

static int
_open_stdin_file(slurmd_job_t *job, task_info_t *t)
{
	int       fd    = -1;
	io_obj_t *obj   = NULL;
	int       flags = O_RDONLY;
	
	if ((fd = _open_task_file(t->ifname, flags)) > 0) {
		obj = _io_obj(job, fd, t->gid, CLIENT_STDIN); 
		_obj_set_unwritable(obj);
		_io_client_attach(obj, NULL, t->in, job->objs);
	}
	return fd;
}

static void
_io_prepare_files(slurmd_job_t *job)
{
	int i;

	if (!job->ofname && !job->efname && !job->ifname)
		return;

	for (i = 0; i < job->ntasks; i++) {
		_open_output_file(job, job->task[i], CLIENT_STDOUT);
		_open_output_file(job, job->task[i], CLIENT_STDERR);
		if (job->ifname)
			_open_stdin_file (job, job->task[i]);
		pthread_kill(job->ioid, SIGHUP);
	}
}

/* Attach io obj "client" as a reader of 'writer' and a writer to 'reader'
 * if 'reader' is NULL client will have no readers.
 *
 */
static void
_io_client_attach(io_obj_t *client, io_obj_t *writer, 
		  io_obj_t *reader, List objList)
{
	struct io_info *src = writer ? writer->arg : NULL;
	struct io_info *dst = reader ? reader->arg : NULL; 
	struct io_info *cli = client->arg;
	struct io_info *io;

	xassert((src != NULL) || (dst != NULL));
	xassert((src == NULL) || (src->magic == IO_MAGIC));
	xassert((dst == NULL) || (dst->magic == IO_MAGIC));

	if (writer == NULL) { 
		debug("connecting %s to reader only", _io_str[cli->type]);
		/* simple case: connect client to reader only and return
		 */
		_io_connect_objs(client, reader);
		list_append(objList, client);
		return;
	}

	io = list_peek(src->readers);
	xassert((io  == NULL) || (io->magic  == IO_MAGIC));

	/* Check to see if src's first reader has disconnected, 
	 * if so, replace the object with this client, if not, 
	 * append client to readers list
	 */
	if ((io != NULL) && (io->disconnected)) {
		/* Resurrect the ghost:
		 * Attached client inherits ghost client's cbuf 
		 * and eof, as well as place in reader list and 
		 * master objList. However, we need to reset the 
		 * file descriptor, operations structure, and 
		 * disconnected flag.
		 */
		xassert(io->obj->fd == -1);
		xassert(io->obj->ops->writable);

		io->obj->fd      = client->fd;
		io->disconnected = 0;

		_ops_destroy(io->obj->ops);
		io->obj->ops     = _ops_copy(client->ops); 

		/* 
		 * Delete old client which is now an empty vessel 
		 */
		list_delete_all(objList, (ListFindF)find_obj, client);

		/*
		 * connect resurrected client ("io") to reader 
		 * if (reader != NULL).
		 */
		if (reader != NULL) 
			_io_connect_objs(io->obj, reader);
		xassert(io->obj->ops->writable == &_writable);
	} else {
		/* Append new client into readers list and master objList
		 * client still copies existing eof bit, though.
		 */
		cli->eof = io ? io->eof : 0;
		/* 
		 * XXX cbuf_replay(io->buf) into client->buf 
		 */
		_io_connect_objs(writer, client);
		if (reader != NULL)
			_io_connect_objs(client, reader);
		list_append(objList, client);
	}

	xassert(_validate_io_list(objList));
}

static void
_io_connect_objs(io_obj_t *obj1, io_obj_t *obj2)
{
	struct io_info *src = (struct io_info *) obj1->arg;
	struct io_info *dst = (struct io_info *) obj2->arg;
	xassert(src->magic == IO_MAGIC);
	xassert(dst->magic == IO_MAGIC);
	if (!list_find_first(src->readers, (ListFindF)find_obj, dst))
		list_append(src->readers, dst);
	if (!list_find_first(dst->writers, (ListFindF)find_obj, src))
		list_append(dst->writers, src);
}

static int
find_fd(void *obj, void *key)
{
	xassert(obj != NULL);
	xassert(key != NULL);

	return (((io_obj_t *)obj)->fd == *((int *)key));
}

static int 
find_obj(void *obj, void *key)
{
	xassert(obj != NULL);
	xassert(key != NULL);

	return (obj == key);
}

/* delete the connection from src to dst, i.e. remove src
 * from dst->writers, and dst from src->readers
 */
static void
_io_disconnect(struct io_info *src, struct io_info *dst)
{
	char *a, *b;
	xassert(src->magic == IO_MAGIC);
	xassert(dst->magic == IO_MAGIC);
	a = _io_str[dst->type];
	b = _io_str[src->type]; 

	if (list_delete_all(src->readers, (ListFindF)find_obj, dst) <= 0)
		error("Unable to delete %s from %s readers list", a, b);

	if (list_delete_all(dst->writers, (ListFindF)find_obj, src) <= 0)
		error("Unable to delete %s from %s writers list", b, a);
}

static void
_io_disconnect_client(struct io_info *client, List objs)
{
	bool   destroy = false;
	struct io_info *t;
	ListIterator    i;

	xassert(client->magic == IO_MAGIC);

	/* Our client becomes a ghost
	 */
	client->disconnected = 1;
		
	if (client->writers) {
		/* delete client from its writer->readers list 
		 */
		i = list_iterator_create(client->writers);
		while ((t = list_next(i))) {
			if (list_count(t->readers) > 1) {
				destroy = true;
				_io_disconnect(client, t);
			}
		}
		list_iterator_destroy(i);
	}

	if (client->readers) {
		/* delete client from its reader->writers list
		 */
		i = list_iterator_create(client->readers);
		while ((t = list_next(i))) {
			if (list_count(t->writers) > 1) {
				_io_disconnect(t, client);
			}
		}
		list_iterator_destroy(i);
	}

	if (destroy) list_delete_all(objs, (ListFindF)find_obj, client);
}

static bool
_isa_task(struct io_info *io)
{
	xassert(io->magic == IO_MAGIC);
	return ((io->type == TASK_STDOUT)
		|| (io->type == TASK_STDERR)
		|| (io->type == TASK_STDIN ));
}

static bool
_isa_client(struct io_info *io)
{
	xassert(io->magic == IO_MAGIC);
	return ((io->type == CLIENT_STDOUT)
		|| (io->type == CLIENT_STDERR)
		|| (io->type == CLIENT_STDIN ));
}

static struct io_operations *
_ops_copy(struct io_operations *ops)
{
	struct io_operations *ret = xmalloc(sizeof(*ops));
	/* 
	 * Copy initial client_ops 
	 */
	*ret = *ops;
	return ret;
}

static void
_ops_destroy(struct io_operations *ops)
{
	xfree(ops);
}

io_obj_t *
_io_obj(slurmd_job_t *job, int fd, uint32_t id, int type)
{
	struct io_info *io = _io_info_create(id);
	struct io_obj *obj = _io_obj_create(fd, (void *)io);

	xassert(io->magic == IO_MAGIC);
	xassert(type >= 0);

	io->type = type;
	switch (type) {
	 case TASK_STDERR:
	 case TASK_STDOUT:
		 obj->ops    = &task_out_ops;
		 io->readers = list_create(NULL);
		 break;
	 case TASK_STDIN:
		 obj->ops    = &task_in_ops;
		 io->buf     = cbuf_create(512, 10240);
		 io->writers = list_create(NULL);
		 break;
	 case CLIENT_STDOUT:
		 io->readers = list_create(NULL);
	 case CLIENT_STDERR:
		 obj->ops    = _ops_copy(&client_ops);
		 io->buf     = cbuf_create(16, 1048576);
		 io->writers = list_create(NULL);
		 break;
	 case CLIENT_STDIN:
		 obj->ops    = _ops_copy(&client_ops);
		 io->readers = list_create(NULL);
		 break;
	 default:
		 error("io: unknown I/O obj type %d", type);
	}

	io->disconnected = fd < 0 ? 1 : 0;

	/* io info pointers back to eio object, 
	 *   job, and task information
	 */
	io->obj  = obj;
	io->job  = job;
	io->task = job->task[io->id - job->task[0]->gid];

	xassert(io->task->gid == io->id);

	return obj;
}

void
io_obj_destroy(io_obj_t *obj)
{
	struct io_info *io = (struct io_info *) obj->arg;

	xassert(obj != NULL);
	xassert(io  != NULL);
	xassert(io->magic == IO_MAGIC);

	switch (io->type) {
         case TASK_STDERR:
	 case TASK_STDOUT:
		 list_destroy(io->readers);
		 break;
	 case TASK_STDIN:
		 cbuf_destroy(io->buf);
		 list_destroy(io->writers);
		 break;
	 case CLIENT_STDOUT:
		 list_destroy(io->readers);
	 case CLIENT_STDERR:
		 cbuf_destroy(io->buf);
		 list_destroy(io->writers);
		 if (obj->ops)
			 xfree(obj->ops);
		 break;
	 case CLIENT_STDIN:
		 xfree(obj->ops);
		 list_destroy(io->readers);
		 break;
	 default:
		 error("unknown IO object type: %ld", io->type);
	}

	xassert(io->magic = ~IO_MAGIC);
	xfree(io);
	xfree(obj);
}

static io_obj_t *
_io_obj_create(int fd, void *arg)
{
	io_obj_t *obj = xmalloc(sizeof(*obj));
	obj->fd  = fd;
	obj->arg = arg;
	obj->ops = NULL;
	return obj;
}

static struct io_info *
_io_info_create(uint32_t id)
{
	struct io_info *io = (struct io_info *) xmalloc(sizeof(*io));
	io->id           = id;
	io->job          = NULL;
	io->task         = NULL;
	io->obj          = NULL;
	io->buf          = NULL;
	io->type         = -1;
	io->readers      = NULL;
	io->writers      = NULL;
	io->eof          = 0;
	io->disconnected = 0;
	xassert(io->magic = IO_MAGIC);
	return io;
}

int
io_init_pipes(slurmd_job_t *job)
{
	int i;
	for (i = 0; i < job->ntasks; i++) {
		if (_io_init_pipes(job->task[i]) == SLURM_FAILURE) {
			error("init_pipes <task %d> failed", i);
			return SLURM_FAILURE;
		}
	}
	return SLURM_SUCCESS;
}

static int
_io_write_header(struct io_info *client, srun_info_t *srun)
{
	slurm_io_stream_header_t hdr;
	char *buf;
	int retval;
	int size   = sizeof(hdr);
	Buf buffer = init_buf(size);

	hdr.version = SLURM_PROTOCOL_VERSION;
	memcpy(hdr.key, srun->key->data, SLURM_SSL_SIGNATURE_LENGTH);
	hdr.task_id = client->id;
	hdr.type    = client->type == CLIENT_STDOUT ? 
				      SLURM_IO_STREAM_INOUT : 
				      SLURM_IO_STREAM_SIGERR;

	pack_io_stream_header(&hdr, buffer);

	/* XXX Shouldn't have to jump through these hoops to 
	 * support slurm Buf type. Need a better way to do this
	 */
	size   = buffer->processed;
	buf    = xfer_buf_data(buffer);
	retval = cbuf_write(client->buf, buf, size, NULL);
	xfree(buf);
	return retval;
}

static int
_io_init_pipes(task_info_t *t)
{
	if (  (pipe(t->pin)  < 0) 
	   || (pipe(t->pout) < 0) 
	   || (pipe(t->perr) < 0) ) {
		error("io_init_pipes: pipe: %m");
		return SLURM_FAILURE;
	}

	fd_set_close_on_exec(t->pin[1]);
	fd_set_close_on_exec(t->pout[0]);
	fd_set_close_on_exec(t->perr[0]);

	fd_set_nonblocking(t->pin[1]);
	fd_set_nonblocking(t->pout[0]);
	fd_set_nonblocking(t->perr[0]);

	return SLURM_SUCCESS;
}

/* prepare for child I/O:
 * dup stdin,stdout,stderr onto appropriate pipes and
 * close write end of stdin, and read end of stdout/err
 */
int 
io_prepare_child(task_info_t *t)
{
	if (dup2(t->pin[0], STDIN_FILENO  ) < 0) {
		error("dup2(stdin): %m");
		return SLURM_FAILURE;
	}

	if (dup2(t->pout[1], STDOUT_FILENO) < 0) {
		error("dup2(stdout): %m");
		return SLURM_FAILURE;
	}

	if (dup2(t->perr[1], STDERR_FILENO) < 0) {
		error("dup2(stderr): %m");
		return SLURM_FAILURE;
	}

	/* ignore errors on close */
	close(t->pin[1] );
	close(t->pout[0]);
	close(t->perr[0]);
	return SLURM_SUCCESS;
}

static void
_obj_close(io_obj_t *obj, List objs)
{
	struct io_info *io = (struct io_info *) obj->arg;

	xassert(io->magic == IO_MAGIC);
	xassert(_validate_io_list(objs));

	debug3("Need to close %d %s", io->id, _io_str[io->type]);

	if (close(obj->fd) < 0)
		error("close: %m");
	obj->fd = -1;

	if (_isa_client(io)) 
		_io_disconnect_client(io, objs);
	else 
		_shutdown_task_obj(io);

	xassert(_validate_io_list(objs));
}

static bool 
_readable(io_obj_t *obj)
{
	bool rc;
	struct io_info *io = (struct io_info *) obj->arg;

	xassert(io->magic == IO_MAGIC);

	rc = (!io->disconnected && !io->eof && (obj->fd > 0));

	return rc;
}

static bool 
_writable(io_obj_t *obj)
{
	bool rc;
	struct io_info *io = (struct io_info *) obj->arg;

	xassert(io->magic == IO_MAGIC);

	debug3("_writable():  task %d fd %d %s [%d %d %d]", 
	       io->id, obj->fd, _io_str[io->type], io->disconnected, 
	       cbuf_used(io->buf), io->eof);

	rc = (!io->disconnected && ((cbuf_used(io->buf) > 0) || io->eof));
	if (rc)
		debug3("%d %s is writable", io->id, _io_str[io->type]);

	return rc;
}

static int
_write(io_obj_t *obj, List objs)
{
	struct io_info *io = (struct io_info *) obj->arg;
	int n = 0;

	xassert(io->magic == IO_MAGIC);
	xassert(io->type >= 0);

	if (io->disconnected)
		return 0;

	debug3("Need to write %ld bytes to %s %d", 
		cbuf_used(io->buf), _io_str[io->type], io->id);

	/* If obj has recvd EOF, and there is no more data to write,
	 * close the descriptor and remove object from event lists
	 */
	if (io->eof && (cbuf_used(io->buf) == 0)) {
		_obj_close(obj, objs);
		return 0;
	}

	while ((n = cbuf_read_to_fd(io->buf, obj->fd, -1)) < 0) {
		if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) 
			break;
		error("write failed: <task %d>: %m", io->id);
		_obj_close(obj, objs);
		return -1;
	}

	debug3("Wrote %d bytes to %s %d", n, _io_str[io->type], io->id);

	return 0;
}

static void
_do_attach(struct io_info *io)
{
	task_info_t *t;
	
	xassert(io != NULL);
	xassert(io->magic == IO_MAGIC);
	xassert((io->type == CLIENT_STDOUT) || (io->type == CLIENT_STDERR));

	io->obj->ops = _ops_copy(&client_ops);

	t = io->task;

	if (io->type == CLIENT_STDOUT) 
		_io_client_attach(io->obj, t->out, t->in, io->job->objs); 
	else 
		_io_client_attach(io->obj, t->err, NULL,  io->job->objs);
}

/* Write method for client objects which are connecting back to the
 * remote host
 */
static int
_connecting_write(io_obj_t *obj, List objs)
{
	struct io_info *io = (struct io_info *) obj->arg;
	int n;

	xassert(io->magic == IO_MAGIC);
	xassert((io->type == CLIENT_STDOUT) || (io->type == CLIENT_STDERR));

	debug3("Need to write %ld bytes to connecting %s %d", 
		cbuf_used(io->buf), _io_str[io->type], io->id);
	while ((n = cbuf_read_to_fd(io->buf, obj->fd, -1)) < 0) {
		if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) 
			continue;
		error("write failed: <task %d>: %m", io->id);
		_obj_close(obj, objs);
		return -1;
	}
	debug3("Wrote %d bytes to %s %d", n, _io_str[io->type], io->id);

	/* If we've written the contents of the buffer, this is
	 * a connecting client no longer -- it may now be attached
	 * to the appropriate task.
	 */
	if (cbuf_used(io->buf) == 0)
		_do_attach(io);

	return 0;
}


static int
_shutdown_task_obj(struct io_info *t)
{
	List l;
	ListIterator i;
	struct io_info *r;

	xassert(_isa_task(t));

	l = (t->type == TASK_STDIN) ? t->writers : t->readers;
	
	i = list_iterator_create(l);
	while ((r = list_next(i))) {
		/* Copy EOF to all readers or writers */
		r->eof = 1;
		/* XXX  When is it ok to destroy a task obj?
		 *      perhaps only if the buffer is empty ...
		 *      but definitely not before then.
		 *
		 * For now, never destroy the task objects
		 *
		 */
		/* list_delete_all(rlist, (ListFindF) find_obj, t); */
	}
	list_iterator_destroy(i);

	return 0;
}

static int
_task_read(io_obj_t *obj, List objs)
{
	struct io_info *r, *t;
	char buf[4096]; /* XXX Configurable? */
	ssize_t n, len = sizeof(buf);
	ListIterator i;

	t = (struct io_info *) obj->arg;

	xassert(t->magic == IO_MAGIC);
	xassert((t->type == TASK_STDOUT) || (t->type == TASK_STDERR));
	xassert(_validate_io_list(objs));

   again:
	if ((n = read(obj->fd, (void *) buf, len)) < 0) {
		if (errno == EINTR)
			goto again;
		if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
		        error("%s %d: read returned EAGAIN",
			       _io_str[t->type], t->id);
			return 0;
		}
		error("Unable to read from task %ld fd %d errno %d %m", 
				t->id, obj->fd, errno);
		return -1;
	}
	debug3("read %d bytes from %s %d", n, _io_str[t->type], t->id);

	if (n == 0) {  /* got eof */
		debug3("got eof on task %ld", t->id);
		_obj_close(obj, objs);
		return 0;
	}

	/* copy buf to all readers */
	i = list_iterator_create(t->readers);
	while((r = list_next(i))) {
		int dropped;
		xassert(r->magic == IO_MAGIC);
		n = cbuf_write(r->buf, (void *) buf, n, &dropped);
		debug3("wrote %ld bytes into %s buf (fd=%d)", 
		       n, _io_str[r->type], r->obj->fd);
		if (dropped > 0) {
			debug3("dropped %ld bytes from %s buf", 
				dropped, _io_str[r->type]);
		}
	}
	list_iterator_destroy(i);

	return 0;
}

static int 
_task_error(io_obj_t *obj, List objs)
{
	struct io_info *t = (struct io_info *) obj->arg;
	xassert(t->magic == IO_MAGIC);
	error("error on %s %d", _io_str[t->type], t->id);
	_obj_close(obj, objs);
	return -1;
}

static int 
_client_read(io_obj_t *obj, List objs)
{
	struct io_info *client = (struct io_info *) obj->arg;
	struct io_info *reader;
	char buf[1024]; /* XXX Configurable? */
	ssize_t n, len = sizeof(buf);
	ListIterator i;

	xassert(client->magic == IO_MAGIC);
	xassert(_validate_io_list(objs));
	xassert(_isa_client(client));
	

   again:
	if ((n = read(obj->fd, (void *) buf, len)) < 0) {
		if (errno == EINTR)
			goto again;
		if ((errno == EAGAIN) || (errno == EWOULDBLOCK))
			fatal("client read");
		error("read from client %ld: %m", client->id);
		return -1;
	}

	debug("read %d bytes from %s %d", n, _io_str[client->type],
			client->id);

	if (n == 0)  { /* got eof, disconnect this client */
		debug3("client %d closed connection", client->id);
		_obj_close(obj, objs);
		return 0;
	}

	if (client->type == CLIENT_STDERR) {
		/* unsigned long int signo = strtoul(buf, NULL, 10); */
		/* return kill(client->id, signo); */
		return 0;
	}

	/* 
	 * Copy cbuf to all readers 
	 */
	i = list_iterator_create(client->readers);
	while((reader = list_next(i))) {
		n = cbuf_write(reader->buf, (void *) buf, n, NULL);
	}
	list_iterator_destroy(i);

	return 0;
}

static int 
_client_error(io_obj_t *obj, List objs)
{
	struct io_info *io = (struct io_info *) obj->arg;

	xassert(io->magic == IO_MAGIC);

	error("%s task %d", _io_str[io->type], io->id); 
	return 0;
}


#ifndef NDEBUG
static void
_validate_task_out(struct io_info *t, int type)
{
	ListIterator i;
	struct io_info *r;

	xassert(t->magic == IO_MAGIC);
	xassert(!t->writers);
	i = list_iterator_create(t->readers);
	while ((r = list_next(i))) {
		xassert(r->magic == IO_MAGIC);
		xassert(r->type == type);
	}
	list_iterator_destroy(i);
}

static void
_validate_task_in(struct io_info *t)
{
	ListIterator i;
	struct io_info *r;

	xassert(t->magic == IO_MAGIC);
	xassert(!t->readers);
	i = list_iterator_create(t->writers);
	while ((r = list_next(i))) {
		xassert(r->magic == IO_MAGIC);
		xassert((r->type == CLIENT_STDOUT) 
		        || (r->type == CLIENT_STDIN));
	}
	list_iterator_destroy(i);
}


static void
_validate_client_stdout(struct io_info *client)
{
	ListIterator i;
	struct io_info *t;

	xassert(client->magic == IO_MAGIC);
	xassert(client->obj->ops->writable != NULL);
	
	i = list_iterator_create(client->readers);
	while ((t = list_next(i))) {
		xassert(t->magic == IO_MAGIC);
		xassert(t->type  == TASK_STDIN);
	}
	list_iterator_destroy(i);

	i = list_iterator_create(client->writers);
	while ((t = list_next(i))) {
		xassert(t->magic == IO_MAGIC);
		xassert(t->type  == TASK_STDOUT);
	}
	list_iterator_destroy(i);
}

static void
_validate_client_stderr(struct io_info *client)
{
	ListIterator i;
	struct io_info *t;

	xassert(client->magic == IO_MAGIC);
	xassert(!client->readers);
	xassert(client->obj->ops->writable != NULL);
	i = list_iterator_create(client->writers);
	while ((t = list_next(i))) {
		xassert(t->magic == IO_MAGIC);
		xassert(t->type  == TASK_STDERR);
	}
	list_iterator_destroy(i);
}

static void
_validate_client_stdin(struct io_info *client)
{
	ListIterator i;
	struct io_info *t;

	xassert(client->magic == IO_MAGIC);
	xassert(!client->writers);
	i = list_iterator_create(client->readers);
	while ((t = list_next(i))) {
		xassert(t->magic == IO_MAGIC);
		xassert(t->type  == TASK_STDIN);
	}
	list_iterator_destroy(i);
}

static int 
_validate_io_list(List objList)
{
	io_obj_t *obj;
	int retval = 1;
	ListIterator i = list_iterator_create(objList);
	while ((obj = list_next(i))) {
		struct io_info *io = (struct io_info *) obj->arg;
		switch (io->type) {
		case TASK_STDOUT:
			_validate_task_out(io, CLIENT_STDOUT);
			break;
		case TASK_STDERR:
			_validate_task_out(io, CLIENT_STDERR);
			break;
		case TASK_STDIN:
			_validate_task_in(io);
			break;
		case CLIENT_STDERR:
			_validate_client_stderr(io);
			break;
		case CLIENT_STDOUT:
			_validate_client_stdout(io);
			break;
		case CLIENT_STDIN:
			_validate_client_stdin(io);
		}
	}
	list_iterator_destroy(i);
	return retval;
}
#endif /* NDEBUG */

