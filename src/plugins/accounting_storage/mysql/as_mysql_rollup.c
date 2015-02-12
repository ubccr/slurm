/*****************************************************************************\
 *  as_mysql_rollup.c - functions for rolling up data for associations
 *                   and machines from the as_mysql storage.
 *****************************************************************************
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2009 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
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

#include "as_mysql_rollup.h"
#include "as_mysql_archive.h"
#include "src/common/parse_time.h"

enum {
	TIME_ALLOC,
	TIME_DOWN,
	TIME_RESV
};

enum {
	ASSOC_TABLES,
	WCKEY_TABLES
};

typedef struct {
	uint32_t count;
	uint32_t id;
	uint64_t time_alloc;
	uint64_t time_down;
	uint64_t time_idle;
	uint64_t time_over;
	uint64_t time_pd;
	uint64_t time_resv;
	uint64_t total_time;
} local_asset_usage_t;

typedef struct {
	int id;
	List loc_assets;
} local_id_usage_t;

typedef struct {
	time_t end;
	int id; /*only needed for reservations */
	List loc_assets;
	time_t start;
} local_cluster_usage_t;

typedef struct {
	uint64_t a_cpu;
	time_t end;
	int id;
	List local_assocs; /* list of assocs to spread unused time
			      over of type local_id_usage_t */
	time_t start;
	uint64_t total_time;
} local_resv_usage_t;

static void _destroy_local_asset_usage(void *object)
{
	local_asset_usage_t *a_usage = (local_asset_usage_t *)object;
	if (a_usage) {
		xfree(a_usage);
	}
}

static void _destroy_local_id_usage(void *object)
{
	local_id_usage_t *a_usage = (local_id_usage_t *)object;
	if (a_usage) {
		xfree(a_usage);
	}
}

static void _destroy_local_cluster_usage(void *object)
{
	local_cluster_usage_t *c_usage = (local_cluster_usage_t *)object;
	if (c_usage) {
		FREE_NULL_LIST(c_usage->loc_assets);
		xfree(c_usage);
	}
}

static void _destroy_local_resv_usage(void *object)
{
	local_resv_usage_t *r_usage = (local_resv_usage_t *)object;
	if (r_usage) {
		if (r_usage->local_assocs)
			list_destroy(r_usage->local_assocs);
		xfree(r_usage);
	}
}

static int _find_loc_asset(void *x, void *key)
{
	local_asset_usage_t *loc_asset = (local_asset_usage_t *)x;
	uint32_t asset_id = *(uint32_t *)key;

	if (loc_asset->id == asset_id)
		return 1;
	return 0;
}

static void _remove_job_asset_time_from_cluster(List c_assets, List j_assets,
						int seconds)
{
	ListIterator c_itr;
	local_asset_usage_t *c_asset, *j_asset;

	if ((seconds <= 0) || !c_assets || !j_assets ||
	    !list_count(c_assets) || !list_count(j_assets))
		return;

	c_itr = list_iterator_create(c_assets);
	while ((c_asset = list_next(c_itr))) {
		if (!(j_asset = list_find_first(
			      j_assets, _find_loc_asset, &c_asset->id)))
			continue;
		c_asset->total_time -= seconds * j_asset->count;
	}
	list_iterator_destroy(c_itr);
}


static void _add_time_asset(List assets, int type, uint32_t id, uint64_t time)
{
	local_asset_usage_t *loc_asset;

	if (!time)
		return;

	loc_asset = list_find_first(assets, _find_loc_asset, &id);

	if (!loc_asset) {
		loc_asset = xmalloc(sizeof(local_asset_usage_t));
		loc_asset->id = id;
		list_append(assets, loc_asset);
	}

	switch (type) {
	case TIME_ALLOC:
		loc_asset->time_alloc += time;
		break;
	case TIME_DOWN:
		loc_asset->time_down += time;
		break;
	case TIME_RESV:
		loc_asset->time_resv += time;
		break;
	default:
		error("_add_time_asset: unknown type %d given", type);
		break;
	}
}

static void _add_job_alloc_time_to_cluster(List c_assets, List j_assets)
{
	ListIterator c_itr = list_iterator_create(c_assets);
	local_asset_usage_t *c_asset, *j_asset;

	while ((c_asset = list_next(c_itr))) {
		if (!(j_asset = list_find_first(j_assets,
						_find_loc_asset, &c_asset->id)))
			continue;
		c_asset->time_alloc += j_asset->time_alloc;
	}
	list_iterator_destroy(c_itr);
}

static void _setup_cluster_asset(List assets, uint32_t id,
				 uint32_t count, int seconds)
{
	local_asset_usage_t *loc_asset =
		list_find_first(assets, _find_loc_asset, &id);

	if (!loc_asset) {
		loc_asset = xmalloc(sizeof(local_asset_usage_t));
		loc_asset->id = id;
		list_append(assets, loc_asset);
	}

	loc_asset->count = count;
	loc_asset->total_time += seconds * loc_asset->count;
}

static int _process_purge(mysql_conn_t *mysql_conn,
			  char *cluster_name,
			  uint16_t archive_data,
			  uint32_t purge_period)
{
	int rc = SLURM_SUCCESS;
	slurmdb_archive_cond_t arch_cond;
	slurmdb_job_cond_t job_cond;

	/* if we didn't ask for archive data return here and don't do
	   anything extra just rollup */

	if (!archive_data)
		return SLURM_SUCCESS;

	if (!slurmdbd_conf)
		return SLURM_SUCCESS;

	memset(&job_cond, 0, sizeof(job_cond));
	memset(&arch_cond, 0, sizeof(arch_cond));
	arch_cond.archive_dir = slurmdbd_conf->archive_dir;
	arch_cond.archive_script = slurmdbd_conf->archive_script;

	if (purge_period & slurmdbd_conf->purge_event)
		arch_cond.purge_event = slurmdbd_conf->purge_event;
	else
		arch_cond.purge_event = NO_VAL;
	if (purge_period & slurmdbd_conf->purge_job)
		arch_cond.purge_job = slurmdbd_conf->purge_job;
	else
		arch_cond.purge_job = NO_VAL;

	if (purge_period & slurmdbd_conf->purge_resv)
		arch_cond.purge_resv = slurmdbd_conf->purge_resv;
	else
		arch_cond.purge_resv = NO_VAL;

	if (purge_period & slurmdbd_conf->purge_step)
		arch_cond.purge_step = slurmdbd_conf->purge_step;
	else
		arch_cond.purge_step = NO_VAL;
	if (purge_period & slurmdbd_conf->purge_suspend)
		arch_cond.purge_suspend = slurmdbd_conf->purge_suspend;
	else
		arch_cond.purge_suspend = NO_VAL;

	job_cond.cluster_list = list_create(NULL);
	list_append(job_cond.cluster_list, cluster_name);

	arch_cond.job_cond = &job_cond;
	rc = as_mysql_jobacct_process_archive(mysql_conn, &arch_cond);
	list_destroy(job_cond.cluster_list);

	return rc;
}

/* static int _process_cluster_asset_nonhour_usage(mysql_conn_t *mysql_conn, */
/* 						char *cluster_name, */
/* 						time_t curr_start, */
/* 						time_t curr_end, */
/* 						time_t use_start, */
/* 						local_asset_usage_t *loc_asset) */
/* { */
/* 	query = xstrdup_printf("insert into \"%s_%s\" " */
/* 			       "(time_start, id_asset, alloc_secs, " */
/* 			       "down_secs, pdown_secs, " */
/* 			       "idle_secs, over_secs, " */
/* 			       "resv_secs) " */
/* 			       "values (%ld, %u, %u, " */
/* 			       "%"PRIu64", %"PRIu64", %"PRIu64", " */
/* 			       "%"PRIu64", %"PRIu64", %"PRIu64") " */
/* 			       "on duplicate key update " */
/* 			       "count=VALUES(count), " */
/* 			       "alloc_secs=VALUES(alloc_secs), " */
/* 			       "down_secs=VALUES(down_secs), " */
/* 			       "pdown_secs=VALUES(pdown_secs), " */
/* 			       "idle_secs=VALUES(idle_secs), " */
/* 			       "over_secs=VALUES(over_secs), " */
/* 			       "resv_secs=VALUES(resv_secs)", */
/* 			       cluster_name, cluster_hour_ext_table, */
/* 			       use_start, loc_asset->id, loc_asset->count, */
/* 			       loc_asset->time_alloc, loc_asset->time_down, */
/* 			       loc_asset->time_pd, loc_asset->time_idle, */
/* 			       loc_asset->time_over, loc_asset->time_resv); */

/* } */

static void _setup_cluster_asset_usage(mysql_conn_t *mysql_conn,
				       char *cluster_name,
				       time_t curr_start, time_t curr_end,
				       time_t now, time_t use_start,
				       local_asset_usage_t *loc_asset,
				       char **query)
{
	char start_char[20], end_char[20];
	uint64_t total_used;

	if (!loc_asset)
		return;

	/* Now put the lists into the usage tables */

	/* sanity check to make sure we don't have more
	   allocated cpus than possible. */
	if (loc_asset->total_time < loc_asset->time_alloc) {
		slurm_make_time_str(&curr_start, start_char,
				    sizeof(start_char));
		slurm_make_time_str(&curr_end, end_char,
				    sizeof(end_char));
		error("We have more allocated time than is "
		      "possible (%"PRIu64" > %"PRIu64") for "
		      "cluster %s(%d) from %s - %s asset %u",
		      loc_asset->time_alloc, loc_asset->total_time,
		      cluster_name, loc_asset->count,
		      start_char, end_char, loc_asset->id);
		loc_asset->time_alloc = loc_asset->total_time;
	}

	total_used = loc_asset->time_alloc +
		loc_asset->time_down + loc_asset->time_pd;

	/* Make sure the total time we care about
	   doesn't go over the limit */
	if (loc_asset->total_time < total_used) {
		int64_t overtime;

		slurm_make_time_str(&curr_start, start_char,
				    sizeof(start_char));
		slurm_make_time_str(&curr_end, end_char,
				    sizeof(end_char));
		error("We have more time than is "
		      "possible (%"PRIu64"+%"PRIu64"+%"
		      PRIu64")(%"PRIu64") > %"PRIu64" for "
		      "cluster %s(%d) from %s - %s asset %u",
		      loc_asset->time_alloc, loc_asset->time_down,
		      loc_asset->time_pd, total_used,
		      loc_asset->total_time,
		      cluster_name, loc_asset->count,
		      start_char, end_char, loc_asset->id);

		/* First figure out how much actual down time
		   we have and then how much
		   planned down time we have. */
		overtime = (int64_t)(loc_asset->total_time -
				     (loc_asset->time_alloc +
				      loc_asset->time_down));
		if (overtime < 0) {
			loc_asset->time_down += overtime;
			if ((int64_t)loc_asset->time_down < 0)
				loc_asset->time_down = 0;
		}

		overtime = (int64_t)(loc_asset->total_time -
				     (loc_asset->time_alloc +
				      loc_asset->time_down +
				      loc_asset->time_pd));
		if (overtime < 0) {
			loc_asset->time_pd += overtime;
			if ((int64_t)loc_asset->time_pd < 0)
				loc_asset->time_pd = 0;
		}

		total_used = loc_asset->time_alloc +
			loc_asset->time_down + loc_asset->time_pd;
		/* info("We now have (%"PRIu64"+%"PRIu64"+" */
		/*      "%"PRIu64")(%"PRIu64") " */
		/*       "?= %"PRIu64"", */
		/*       loc_asset->time_alloc, loc_asset->time_down, */
		/*       loc_asset->time_pd, total_used, */
		/*       loc_asset->total_time); */
	}

	loc_asset->time_idle = loc_asset->total_time -
		total_used - loc_asset->time_resv;
	/* sanity check just to make sure we have a
	 * legitimate time after we calulated
	 * idle/reserved time put extra in the over
	 * commit field
	 */
	/* info("%s got idle of %lld", loc_asset->name, */
	/*      (int64_t)loc_asset->time_idle); */
	if ((int64_t)loc_asset->time_idle < 0) {
		/* info("got %d %d %d", loc_asset->time_resv, */
		/*      loc_asset->time_idle, loc_asset->time_over); */
		loc_asset->time_resv += (int64_t)loc_asset->time_idle;
		loc_asset->time_over -= (int64_t)loc_asset->time_idle;
		loc_asset->time_idle = 0;
		if ((int64_t)loc_asset->time_resv < 0)
			loc_asset->time_resv = 0;
	}

	/* info("cluster %s(%u) down %"PRIu64" alloc %"PRIu64" " */
	/*      "resv %"PRIu64" idle %"PRIu64" over %"PRIu64" " */
	/*      "total= %"PRIu64" ?= %"PRIu64" from %s", */
	/*      cluster_name, */
	/*      loc_asset->count, loc_asset->time_down, */
	/*      loc_asset->time_alloc, */
	/*      loc_asset->time_resv, loc_asset->time_idle, */
	/*      loc_asset->time_over, */
	/*      loc_asset->time_down + loc_asset->time_alloc + */
	/*      loc_asset->time_resv + loc_asset->time_idle, */
	/*      loc_asset->total_time, */
	/*      slurm_ctime(&loc_asset->start)); */
	/* info("to %s", slurm_ctime(&loc_asset->end)); */
	if (*query)
		xstrfmtcat(*query, ", (%ld, %ld, %ld, %u, %u, "
			   "%"PRIu64", %"PRIu64", %"PRIu64", "
			   "%"PRIu64", %"PRIu64", %"PRIu64")",
			   now, now, use_start, loc_asset->id,
			   loc_asset->count,
			   loc_asset->time_alloc,
			   loc_asset->time_down,
			   loc_asset->time_pd,
			   loc_asset->time_idle,
			   loc_asset->time_over,
			   loc_asset->time_resv);
	else
		xstrfmtcat(*query, "insert into \"%s_%s\" "
			   "(creation_time, mod_time, "
			   "time_start, id_asset, count, "
			   "alloc_secs, down_secs, pdown_secs, "
			   "idle_secs, over_secs, resv_secs) "
			   "values (%ld, %ld, %ld, %u, %u, "
			   "%"PRIu64", %"PRIu64", %"PRIu64", "
			   "%"PRIu64", %"PRIu64", %"PRIu64")",
			   cluster_name, cluster_hour_table,
			   now, now,
			   use_start, loc_asset->id,
			   loc_asset->count,
			   loc_asset->time_alloc,
			   loc_asset->time_down,
			   loc_asset->time_pd,
			   loc_asset->time_idle,
			   loc_asset->time_over,
			   loc_asset->time_resv);

	return;
}

static int _process_cluster_usage(mysql_conn_t *mysql_conn,
				  char *cluster_name,
				  time_t curr_start, time_t curr_end,
				  time_t now, local_cluster_usage_t *c_usage)
{
	int rc = SLURM_SUCCESS;
	char *query = NULL;
	ListIterator itr;
	local_asset_usage_t *loc_asset;

	if (!c_usage)
		return rc;
	/* Now put the lists into the usage tables */

	xassert(c_usage->loc_assets);
	itr = list_iterator_create(c_usage->loc_assets);
	while ((loc_asset = list_next(itr))) {
		_setup_cluster_asset_usage(mysql_conn, cluster_name,
					   curr_start, curr_end, now,
					   c_usage->start, loc_asset, &query);
	}
	list_iterator_destroy(itr);

	if (!query)
		return rc;

	xstrfmtcat(query,
		   " on duplicate key update "
		   "mod_time=%ld, count=VALUES(count), "
		   "alloc_secs=VALUES(alloc_secs), "
		   "down_secs=VALUES(down_secs), "
		   "pdown_secs=VALUES(pdown_secs), "
		   "idle_secs=VALUES(idle_secs), "
		   "over_secs=VALUES(over_secs), "
		   "resv_secs=VALUES(resv_secs)",
		   now);

	/* Spacing out the inserts here instead of doing them
	   all at once in the end proves to be faster.  Just FYI
	   so we don't go testing again and again.
	*/
	if (debug_flags & DEBUG_FLAG_DB_USAGE)
		DB_DEBUG(mysql_conn->conn, "query\n%s", query);
	rc = mysql_db_query(mysql_conn, query);
	xfree(query);
	if (rc != SLURM_SUCCESS)
		error("Couldn't add cluster hour rollup");

	return rc;
}

/* static void _create_id_usage_nonhour_insert(char *cluster_name, int type, */
/* 					    time_t curr_start, time_t now, */
/* 					    local_id_usage_t *id_usage, */
/* 					    char **query) */
/* { */
/* 	switch (type) { */
/* 	case ASSOC_TABLES: */
/* 		id_name = "id_assoc"; */
/* 		table = assoc_day_table; */
/* 		table_ext = assoc_day_ext_table; */
/* 		break; */
/* 	case WCKEY_TABLES: */
/* 		id_name = "id_wckey"; */
/* 		table = wckey_day_table; */
/* 		table_ext = wckey_day_ext_table; */
/* 		break; */
/* 	default: */
/* 		error("_create_id_usage_nonhour_insert: unknown type %d", type); */
/* 		return; */
/* 		break; */
/* 	} */

/* 	xstrfmtcat(*query, */
/* 		   "insert into \"%s_%s\" (creation_time, mod_time, %s, " */
/* 		   "time_start) " */
/* 		   "values (%ld, %ld, %u, %ld, %"PRIu64") " */
/* 		   "on duplicate key update mod_time=VALUES(mod_time), " */
/* 		   "inx=LAST_INSERT_ID(inx);", */
/* 		   cluster_name, table, id_name, now, now, */
/* 		   id_usage->id, curr_start); */

/* 	if (!id_usage->loc_assets || !list_count(id_usage->loc_assets)) { */
/* 		error("%s %d doesn't have any assets", id_name, id_usage->id); */
/* 		return; */
/* 	} */

/* 	first = 1; */
/* 	itr = list_iterator_create(id_usage->loc_assets); */
/* 	while ((loc_asset = list_next(itr))) { */
/* 		if (!first) { */
/* 			xstrfmtcat(*query, */
/* 				   ", (LAST_INSERT_ID(), %u, %"PRIu64");", */
/* 				   loc_asset->id, loc_asset->time_alloc); */

/* 		} else { */
/* 			first = 0; */
/* 			xstrfmtcat(*query, */
/* 				   "insert into \"%s_%s\" " */
/* 				   "(inx, id_asset, alloc_secs) " */
/* 				   "values (LAST_INSERT_ID(), %u, %"PRIu64")", */
/* 				   cluster_name, table_ext, */
/* 				   loc_asset->id, loc_asset->time_alloc); */
/* 		} */
/* 	} */
/* 	list_iterator_destroy(itr); */
/* 	xstrcat(*query, */
/* 		" on duplicate key update alloc_secs=VALUES(alloc_secs);"); */
/* } */

static void _create_id_usage_insert(char *cluster_name, int type,
				    time_t curr_start, time_t now,
				    local_id_usage_t *id_usage,
				    char **query)
{
	local_asset_usage_t *loc_asset;
	ListIterator itr;
	bool first;
	char *table = NULL, *id_name = NULL;

	xassert(query);

	switch (type) {
	case ASSOC_TABLES:
		id_name = "id_assoc";
		table = assoc_hour_table;
		break;
	case WCKEY_TABLES:
		id_name = "id_wckey";
		table = wckey_hour_table;
		break;
	default:
		error("_create_id_usage_insert: unknown type %d", type);
		return;
		break;
	}

	if (!id_usage->loc_assets || !list_count(id_usage->loc_assets)) {
		error("%s %d doesn't have any assets", id_name, id_usage->id);
		return;
	}

	first = 1;
	itr = list_iterator_create(id_usage->loc_assets);
	while ((loc_asset = list_next(itr))) {
		if (!first) {
			xstrfmtcat(*query,
				   ", (%ld, %ld, %u, %ld, %u, %"PRIu64")",
				   now, now,
				   id_usage->id, curr_start, loc_asset->id,
				   loc_asset->time_alloc);
		} else {
			xstrfmtcat(*query,
				   "insert into \"%s_%s\" "
				   "(creation_time, mod_time, id, "
				   "time_start, id_asset, alloc_secs) "
				   "values (%ld, %ld, %u, %ld, %u, %"PRIu64")",
				   cluster_name, table, now, now,
				   id_usage->id, curr_start, loc_asset->id,
				   loc_asset->time_alloc);
			first = 0;
		}
	}
	list_iterator_destroy(itr);
	xstrfmtcat(*query,
		   " on duplicate key update mod_time=%ld, "
		   "alloc_secs=VALUES(alloc_secs);", now);
}

/* assoc_mgr_locks on assets need to be read locked before this */
static local_cluster_usage_t *_setup_cluster_usage(mysql_conn_t *mysql_conn,
						   char *cluster_name,
						   time_t curr_start,
						   time_t curr_end,
						   List cluster_down_list)
{
	local_cluster_usage_t *c_usage = NULL;
	char *query = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	int i = 0;
	ListIterator itr2 = NULL;
	slurmdb_asset_rec_t *asset_rec;

	char *event_req_inx[] = {
		"node_name",
		"time_start",
		"time_end",
		"state",
	};
	char *event_str = NULL;
	enum {
		EVENT_REQ_NAME,
		EVENT_REQ_START,
		EVENT_REQ_END,
		EVENT_REQ_STATE,
		EVENT_REQ_COUNT
	};

	xstrfmtcat(event_str, "%s", event_req_inx[i]);
	for(i=1; i<EVENT_REQ_COUNT; i++) {
		xstrfmtcat(event_str, ", %s", event_req_inx[i]);
	}

	xstrcat(event_str, full_asset_query);

	/* first get the events during this time.  All that is
	 * except things with the maintainance flag set in the
	 * state.  We handle those later with the reservations.
	 */
	query = xstrdup_printf("select %s from \"%s_%s\" where "
			       "!(state & %d) && (time_start < %ld "
			       "&& (time_end >= %ld "
			       "|| time_end = 0)) "
			       "order by node_name, time_start",
			       event_str, cluster_name, event_view,
			       NODE_STATE_MAINT,
			       curr_end, curr_start);
	xfree(event_str);

	if (debug_flags & DEBUG_FLAG_DB_USAGE)
		DB_DEBUG(mysql_conn->conn, "query\n%s", query);
	if (!(result = mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return NULL;
	}
	xfree(query);

	itr2 = list_iterator_create(assoc_mgr_asset_list);
	while ((row = mysql_fetch_row(result))) {
		time_t row_start = slurm_atoul(row[EVENT_REQ_START]);
		time_t row_end = slurm_atoul(row[EVENT_REQ_END]);
		uint16_t state = slurm_atoul(row[EVENT_REQ_STATE]);
		local_asset_usage_t *loc_asset;

		if (row_start < curr_start)
			row_start = curr_start;

		if (!row_end || row_end > curr_end)
			row_end = curr_end;

		/* Don't worry about it if the time is less
		 * than 1 second.
		 */
		if ((row_end - row_start) < 1)
			continue;

		/* this means we are a cluster registration
		   entry */
		if (!row[EVENT_REQ_NAME][0]) {
			local_cluster_usage_t *loc_c_usage = c_usage;

			/* if the cpu count changes we will
			 * only care about the last cpu count but
			 * we will keep a total of the time for
			 * all cpus to get the correct cpu time
			 * for the entire period.
			 */

			if (state || !c_usage) {
				loc_c_usage = xmalloc(
					sizeof(local_cluster_usage_t));
				loc_c_usage->start = row_start;
				loc_c_usage->loc_assets =
					list_create(_destroy_local_asset_usage);
				/* If this has a state it
				   means the slurmctld went
				   down and we should put this
				   on the list and remove any
				   jobs from this time that
				   were running later.
				*/
				if (state)
					list_append(cluster_down_list,
						    loc_c_usage);
				else
					c_usage = loc_c_usage;
			} else
				loc_c_usage = c_usage;

			loc_c_usage->end = row_end;

			xassert(loc_c_usage->loc_assets);

			i = EVENT_REQ_COUNT-1;
			list_iterator_reset(itr2);
			while ((asset_rec = list_next(itr2))) {
				i++;
				/* Skip if the asset is NULL,
				 * it means this cluster
				 * doesn't care about it.
				 */
				if (!row[i] || !row[i][0])
					continue;

				_setup_cluster_asset(loc_c_usage->loc_assets,
						     asset_rec->id,
						     slurm_atoul(row[i]),
						     (row_end - row_start));
			}

			continue;
		}

		/* only record down time for the cluster we
		   are looking for.  If it was during this
		   time period we would already have it.
		*/
		if (c_usage) {
			int local_start = row_start;
			int local_end = row_end;
			int seconds;
			if (c_usage->start > local_start)
				local_start = c_usage->start;
			if (c_usage->end < local_end)
				local_end = c_usage->end;
			seconds = (local_end - local_start);
			if (seconds > 0) {
				ListIterator c_itr = list_iterator_create(
					c_usage->loc_assets);

				while ((loc_asset = list_next(c_itr))) {
					/* info("node %s adds asset %d " */
					/*      "(%d)(%d-%d) * %d = %d " */
					/*      "to %d", */
					/*      row[EVENT_REQ_NAME], */
					/*      loc_asset->id, seconds, */
					/*      local_end, local_start, */
					/*      c_asset->count, */
					/*      seconds * c_asset->count, */
					/*      c_asset->count); */
					loc_asset->time_down +=
						seconds * loc_asset->count;
				}
				list_iterator_destroy(c_itr);
			}
		}
	}
	mysql_free_result(result);

	list_iterator_destroy(itr2);

	return c_usage;
}

extern int as_mysql_hourly_rollup(mysql_conn_t *mysql_conn,
				  char *cluster_name,
				  time_t start, time_t end,
				  uint16_t archive_data)
{
	int rc = SLURM_SUCCESS;
	int add_sec = 3600;
	int i=0, id;
	time_t now = time(NULL);
	time_t curr_start = start;
	time_t curr_end = curr_start + add_sec;
	char *query = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	ListIterator itr2 = NULL;
	ListIterator a_itr = NULL;
	ListIterator c_itr = NULL;
	ListIterator w_itr = NULL;
	ListIterator r_itr = NULL;
	List assoc_usage_list = list_create(_destroy_local_id_usage);
	List cluster_down_list = list_create(_destroy_local_cluster_usage);
	List wckey_usage_list = list_create(_destroy_local_id_usage);
	List resv_usage_list = list_create(_destroy_local_resv_usage);
	uint16_t track_wckey = slurm_get_track_wckey();
	assoc_mgr_lock_t locks = { READ_LOCK, NO_LOCK, NO_LOCK,
				   NO_LOCK, NO_LOCK, NO_LOCK, NO_LOCK };
	/* char start_char[20], end_char[20]; */

	char *job_req_inx[] = {
		"job.job_db_inx",
		"job.id_job",
		"job.id_assoc",
		"job.id_wckey",
		"job.array_task_pending",
		"job.time_eligible",
		"job.time_start",
		"job.time_end",
		"job.time_suspended",
		"job.cpus_req",
		"job.id_resv",
		"SUM(step.consumed_energy)"
	};
	char *job_str = NULL;
	enum {
		JOB_REQ_DB_INX,
		JOB_REQ_JOBID,
		JOB_REQ_ASSOCID,
		JOB_REQ_WCKEYID,
		JOB_REQ_ARRAY_PENDING,
		JOB_REQ_ELG,
		JOB_REQ_START,
		JOB_REQ_END,
		JOB_REQ_SUSPENDED,
		JOB_REQ_RCPU,
		JOB_REQ_RESVID,
		JOB_REQ_ENERGY,
		JOB_REQ_COUNT
	};

	char *suspend_req_inx[] = {
		"time_start",
		"time_end"
	};
	char *suspend_str = NULL;
	enum {
		SUSPEND_REQ_START,
		SUSPEND_REQ_END,
		SUSPEND_REQ_COUNT
	};

	char *resv_req_inx[] = {
		"id_resv",
		"assoclist",
		"cpus",
		"flags",
		"time_start",
		"time_end"
	};
	char *resv_str = NULL;
	enum {
		RESV_REQ_ID,
		RESV_REQ_ASSOCS,
		RESV_REQ_CPU,
		RESV_REQ_FLAGS,
		RESV_REQ_START,
		RESV_REQ_END,
		RESV_REQ_COUNT
	};

	i=0;
	xstrfmtcat(job_str, "%s", job_req_inx[i]);
	for(i=1; i<JOB_REQ_COUNT; i++) {
		xstrfmtcat(job_str, ", %s", job_req_inx[i]);
	}

	i=0;
	xstrfmtcat(suspend_str, "%s", suspend_req_inx[i]);
	for(i=1; i<SUSPEND_REQ_COUNT; i++) {
		xstrfmtcat(suspend_str, ", %s", suspend_req_inx[i]);
	}

	i=0;
	xstrfmtcat(resv_str, "%s", resv_req_inx[i]);
	for(i=1; i<RESV_REQ_COUNT; i++) {
		xstrfmtcat(resv_str, ", %s", resv_req_inx[i]);
	}
	assoc_mgr_lock(&locks);

	xstrcat(job_str, full_asset_query);

/* 	info("begin start %s", slurm_ctime(&curr_start)); */
/* 	info("begin end %s", slurm_ctime(&curr_end)); */
	itr2 = list_iterator_create(assoc_mgr_asset_list);
	a_itr = list_iterator_create(assoc_usage_list);
	c_itr = list_iterator_create(cluster_down_list);
	w_itr = list_iterator_create(wckey_usage_list);
	r_itr = list_iterator_create(resv_usage_list);
	while (curr_start < end) {
		int last_id = -1;
		int last_wckeyid = -1;
		int seconds = 0;
		int tot_time = 0;
		local_cluster_usage_t *loc_c_usage = NULL;
		local_cluster_usage_t *c_usage = NULL;
		local_resv_usage_t *r_usage = NULL;
		local_id_usage_t *a_usage = NULL;
		local_id_usage_t *w_usage = NULL;

		if (debug_flags & DEBUG_FLAG_DB_USAGE)
			DB_DEBUG(mysql_conn->conn,
				 "%s curr hour is now %ld-%ld",
				 cluster_name, curr_start, curr_end);
/* 		info("start %s", slurm_ctime(&curr_start)); */
/* 		info("end %s", slurm_ctime(&curr_end)); */

		c_usage = _setup_cluster_usage(mysql_conn, cluster_name,
					       curr_start, curr_end,
					       cluster_down_list);

		// now get the reservations during this time
		/* If a reservation has the IGNORE_JOBS flag we don't
		 * have an easy way to distinguish the cpus a job not
		 * running in the reservation, but on it's cpus.
		 * So we will just ignore these reservations for
		 * accounting purposes.
		 */
		query = xstrdup_printf("select %s from \"%s_%s\" where "
				       "(time_start < %ld && time_end >= %ld) "
				       "&& !(flags & %u)"
				       "order by time_start",
				       resv_str, cluster_name, resv_table,
				       curr_end, curr_start,
				       RESERVE_FLAG_IGN_JOBS);

		if (debug_flags & DEBUG_FLAG_DB_USAGE)
			DB_DEBUG(mysql_conn->conn, "query\n%s", query);
		if (!(result = mysql_db_query_ret(
			      mysql_conn, query, 0))) {
			xfree(query);
			_destroy_local_cluster_usage(c_usage);
			rc = SLURM_ERROR;
			goto end_it;
		}
		xfree(query);

		if (c_usage)
			xassert(c_usage->loc_assets);

		/* If a reservation overlaps another reservation we
		   total up everything here as if they didn't but when
		   calculating the total time for a cluster we will
		   remove the extra time received.  This may result in
		   unexpected results with association based reports
		   since the association is given the total amount of
		   time of each reservation, thus equaling more time
		   than is available.  Job/Cluster/Reservation reports
		   should be fine though since we really don't over
		   allocate resources.  The issue with us not being
		   able to handle overlapping reservations here is
		   unless the reservation completely overlaps the
		   other reservation we have no idea how many cpus
		   should be removed since this could be a
		   heterogeneous system.  This same problem exists
		   when a reservation is created with the ignore_jobs
		   option which will allow jobs to continue to run in the
		   reservation that aren't suppose to.
		*/
		while ((row = mysql_fetch_row(result))) {
			time_t row_start = slurm_atoul(row[RESV_REQ_START]);
			time_t row_end = slurm_atoul(row[RESV_REQ_END]);
			uint32_t row_cpu = slurm_atoul(row[RESV_REQ_CPU]);
			uint32_t row_flags = slurm_atoul(row[RESV_REQ_FLAGS]);
			local_asset_usage_t *loc_asset;

			if (row_start < curr_start)
				row_start = curr_start;

			if (!row_end || row_end > curr_end)
				row_end = curr_end;

			/* Don't worry about it if the time is less
			 * than 1 second.
			 */
			if ((row_end - row_start) < 1)
				continue;

			r_usage = xmalloc(sizeof(local_resv_usage_t));
			r_usage->id = slurm_atoul(row[RESV_REQ_ID]);

			r_usage->local_assocs = list_create(slurm_destroy_char);
			slurm_addto_char_list(r_usage->local_assocs,
					      row[RESV_REQ_ASSOCS]);

			r_usage->total_time = (row_end - row_start) * row_cpu;
			r_usage->start = row_start;
			r_usage->end = row_end;
			list_append(resv_usage_list, r_usage);

			/* Since this reservation was added to the
			   cluster and only certain people could run
			   there we will use this as allocated time on
			   the system.  If the reservation was a
			   maintenance then we add the time to planned
			   down time.
			*/


			/* only record time for the clusters that have
			   registered.  This continue should rarely if
			   ever happen.
			*/
			if (!c_usage)
				continue;

			/* Only able to do CPUs currently */
			id = ASSET_CPU;
			loc_asset = list_find_first(c_usage->loc_assets,
						    _find_loc_asset, &id);

			if (row_flags & RESERVE_FLAG_MAINT)
				loc_asset->time_pd += r_usage->total_time;
			else
				loc_asset->time_alloc += r_usage->total_time;

			/* slurm_make_time_str(&r_usage->start, start_char, */
			/* 		    sizeof(start_char)); */
			/* slurm_make_time_str(&r_usage->end, end_char, */
			/* 		    sizeof(end_char)); */
			/* info("adding this much %lld to cluster %s " */
			/*      "%d %d %s - %s", */
			/*      r_usage->total_time, c_usage->name, */
			/*      (row_flags & RESERVE_FLAG_MAINT),  */
			/*      r_usage->id, start_char, end_char); */
		}
		mysql_free_result(result);

		/* now get the jobs during this time only  */
		query = xstrdup_printf("select %s from \"%s_%s\" as job "
				       "left outer join \"%s_%s\" as step on "
				       "job.job_db_inx=step.job_db_inx "
				       "and (step.id_step>=0) "
				       "where (job.time_eligible < %ld && "
				       "(job.time_end >= %ld || "
				       "job.time_end = 0)) "
				       "group by job.job_db_inx "
				       "order by job.id_assoc, "
				       "job.time_eligible",
				       job_str, cluster_name, job_view,
				       cluster_name, step_table,
				       curr_end, curr_start);

		if (debug_flags & DEBUG_FLAG_DB_USAGE)
			DB_DEBUG(mysql_conn->conn, "query\n%s", query);
		if (!(result = mysql_db_query_ret(
			      mysql_conn, query, 0))) {
			xfree(query);
			_destroy_local_cluster_usage(c_usage);
			rc = SLURM_ERROR;
			goto end_it;
		}
		xfree(query);

		while ((row = mysql_fetch_row(result))) {
			uint32_t job_id = slurm_atoul(row[JOB_REQ_JOBID]);
			uint32_t assoc_id = slurm_atoul(row[JOB_REQ_ASSOCID]);
			uint32_t wckey_id = slurm_atoul(row[JOB_REQ_WCKEYID]);
			uint32_t array_pending =
				slurm_atoul(row[JOB_REQ_ARRAY_PENDING]);
			uint32_t resv_id = slurm_atoul(row[JOB_REQ_RESVID]);
			time_t row_eligible = slurm_atoul(row[JOB_REQ_ELG]);
			time_t row_start = slurm_atoul(row[JOB_REQ_START]);
			time_t row_end = slurm_atoul(row[JOB_REQ_END]);
			uint32_t row_rcpu = slurm_atoul(row[JOB_REQ_RCPU]);
			slurmdb_asset_rec_t *asset_rec;
			uint64_t row_energy = 0;
			int loc_seconds = 0;
			seconds = 0;

			if (row[JOB_REQ_ENERGY])
				row_energy = slurm_atoull(row[JOB_REQ_ENERGY]);
			if (row_start && (row_start < curr_start))
				row_start = curr_start;

			if (!row_start && row_end)
				row_start = row_end;

			if (!row_end || row_end > curr_end)
				row_end = curr_end;

			if (!row_start || ((row_end - row_start) < 1))
				goto calc_cluster;

			seconds = (row_end - row_start);

			if (slurm_atoul(row[JOB_REQ_SUSPENDED])) {
				MYSQL_RES *result2 = NULL;
				MYSQL_ROW row2;
				/* get the suspended time for this job */
				query = xstrdup_printf(
					"select %s from \"%s_%s\" where "
					"(time_start < %ld && (time_end >= %ld "
					"|| time_end = 0)) && job_db_inx=%s "
					"order by time_start",
					suspend_str, cluster_name,
					suspend_table,
					curr_end, curr_start,
					row[JOB_REQ_DB_INX]);

				debug4("%d(%s:%d) query\n%s",
				       mysql_conn->conn, THIS_FILE,
				       __LINE__, query);
				if (!(result2 = mysql_db_query_ret(
					      mysql_conn,
					      query, 0))) {
					xfree(query);
					_destroy_local_cluster_usage(c_usage);
					assoc_mgr_unlock(&locks);
					return SLURM_ERROR;
				}
				xfree(query);
				while ((row2 = mysql_fetch_row(result2))) {
					time_t local_start = slurm_atoul(
						row2[SUSPEND_REQ_START]);
					time_t local_end = slurm_atoul(
						row2[SUSPEND_REQ_END]);

					if (!local_start)
						continue;

					if (row_start > local_start)
						local_start = row_start;
					if (row_end < local_end)
						local_end = row_end;
					tot_time = (local_end - local_start);
					if (tot_time < 1)
						continue;

					seconds -= tot_time;
				}
				mysql_free_result(result2);
			}
			if (seconds < 1) {
				debug4("This job (%u) was suspended "
				       "the entire hour", job_id);
				continue;
			}

			if (last_id != assoc_id) {
				a_usage = xmalloc(sizeof(local_id_usage_t));
				a_usage->id = assoc_id;
				list_append(assoc_usage_list, a_usage);
				last_id = assoc_id;
				a_usage->loc_assets = list_create(
					_destroy_local_asset_usage);
			}

			/* Short circuit this so so we don't get a pointer. */
			if (!track_wckey)
				last_wckeyid = wckey_id;

			/* do the wckey calculation */
			if (last_wckeyid != wckey_id) {
				list_iterator_reset(w_itr);
				while ((w_usage = list_next(w_itr)))
					if (w_usage->id == wckey_id)
						break;

				if (!w_usage) {
					w_usage = xmalloc(
						sizeof(local_id_usage_t));
					w_usage->id = wckey_id;
					list_append(wckey_usage_list,
						    w_usage);
					w_usage->loc_assets = list_create(
						_destroy_local_asset_usage);
				}
				last_wckeyid = wckey_id;
			}

			i = JOB_REQ_COUNT-1;
			list_iterator_reset(itr2);
			while ((asset_rec = list_next(itr2))) {
				uint64_t time;
				i++;
				/* Skip if the asset is NULL,
				 * it means this cluster
				 * doesn't care about it.
				 */
				if (!row[i] || !row[i][0])
					continue;
				time = slurm_atoul(row[i]) * seconds;
				_add_time_asset(a_usage->loc_assets,
						TIME_ALLOC, asset_rec->id,
						time);
				if (w_usage)
					_add_time_asset(w_usage->loc_assets,
							TIME_ALLOC,
							asset_rec->id,
							time);
			}

			_add_time_asset(a_usage->loc_assets,
					TIME_ALLOC, ASSET_ENERGY,
					row_energy);
			if (w_usage)
				_add_time_asset(
					w_usage->loc_assets,
					TIME_ALLOC, ASSET_ENERGY,
					row_energy);

			/* do the cluster allocated calculation */
		calc_cluster:

			/* Now figure out there was a disconnected
			   slurmctld durning this job.
			*/
			list_iterator_reset(c_itr);
			while ((loc_c_usage = list_next(c_itr))) {
				int temp_end = row_end;
				int temp_start = row_start;
				if (loc_c_usage->start > temp_start)
					temp_start = loc_c_usage->start;
				if (loc_c_usage->end < temp_end)
					temp_end = loc_c_usage->end;
				loc_seconds = (temp_end - temp_start);
				/* info(" Job %u was running for " */
				/*      "%"PRIu64" seconds while " */
				/*      "cluster %s's slurmctld " */
				/*      "wasn't responding", */
				/*      job_id, */
				/*      (uint64_t) */
				/*      (seconds * row_acpu), */
				/*      cluster_name); */
				_remove_job_asset_time_from_cluster(
					loc_c_usage->loc_assets,
					a_usage->loc_assets,
					loc_seconds);
			}

			/* first figure out the reservation */
			if (resv_id) {
				if (seconds <= 0)
					continue;
				/* Since we have already added the
				   entire reservation as used time on
				   the cluster we only need to
				   calculate the used time for the
				   reservation and then divy up the
				   unused time over the associations
				   able to run in the reservation.
				   Since the job was to run, or ran a
				   reservation we don't care about
				   eligible time since that could
				   totally skew the clusters reserved time
				   since the job may be able to run
				   outside of the reservation. */
				list_iterator_reset(r_itr);
				while ((r_usage = list_next(r_itr))) {
					int temp_end, temp_start;
					/* since the reservation could
					   have changed in some way,
					   thus making a new
					   reservation record in the
					   database, we have to make
					   sure all the reservations
					   are checked to see if such
					   a thing has happened */
					if (r_usage->id != resv_id)
						continue;
					temp_end = row_end;
					temp_start = row_start;
					if (r_usage->start > temp_start)
						temp_start =
							r_usage->start;
					if (r_usage->end < temp_end)
						temp_end = r_usage->end;

					loc_seconds = (temp_end - temp_start);

					if (loc_seconds > 0) {
						id = ASSET_CPU;
						local_asset_usage_t *loc_asset =
							list_find_first(
								a_usage->
								loc_assets,
								_find_loc_asset,
								&id);
						if (loc_asset) {
							r_usage->a_cpu +=
								loc_seconds *
								loc_asset->
								count;
						}
					}
				}
				continue;
			}

			/* only record time for the clusters that have
			   registered.  This continue should rarely if
			   ever happen.
			*/
			if (!c_usage)
				continue;

			if (row_start && (seconds > 0)) {
				/* info("%d assoc %d adds " */
				/*      "(%d)(%d-%d) * %d = %d " */
				/*      "to %d", */
				/*      job_id, */
				/*      a_usage->id, */
				/*      seconds, */
				/*      row_end, row_start, */
				/*      row_acpu, */
				/*      seconds * row_acpu, */
				/*      row_acpu); */

				_add_job_alloc_time_to_cluster(
					c_usage->loc_assets,
					a_usage->loc_assets);
			}

			/* now reserved time */
			if (!row_start || (row_start >= c_usage->start)) {
				int temp_end = row_start;
				int temp_start = row_eligible;
				if (c_usage->start > temp_start)
					temp_start = c_usage->start;
				if (c_usage->end < temp_end)
					temp_end = c_usage->end;
				loc_seconds = (temp_end - temp_start);
				if (loc_seconds > 0) {
					/* If we have pending jobs in
					   an array they haven't been
					   inserted into the database
					   yet as proper job records,
					   so handle them here.
					*/
					if (array_pending)
						loc_seconds *= array_pending;

					/* info("%d assoc %d reserved " */
					/*      "(%d)(%d-%d) * %d * %d = %d " */
					/*      "to %d", */
					/*      job_id, */
					/*      assoc_id, */
					/*      temp_end - temp_start, */
					/*      temp_end, temp_start, */
					/*      row_rcpu, */
					/*      array_pending, */
					/*      loc_seconds, */
					/*      row_rcpu); */

					_add_time_asset(c_usage->loc_assets,
							TIME_RESV, ASSET_CPU,
							loc_seconds * row_rcpu);
				}
			}
		}
		mysql_free_result(result);

		/* now figure out how much more to add to the
		   associations that could had run in the reservation
		*/
		list_iterator_reset(r_itr);
		while ((r_usage = list_next(r_itr))) {
			int64_t idle = r_usage->total_time - r_usage->a_cpu;
			char *assoc = NULL;
			ListIterator tmp_itr = NULL;

			if (idle <= 0)
				continue;

			/* now divide that time by the number of
			   associations in the reservation and add
			   them to each association */
			seconds = idle / list_count(r_usage->local_assocs);
/* 			info("resv %d got %d for seconds for %d assocs", */
/* 			     r_usage->id, seconds, */
/* 			     list_count(r_usage->local_assocs)); */
			tmp_itr = list_iterator_create(r_usage->local_assocs);
			while ((assoc = list_next(tmp_itr))) {
				uint32_t associd = slurm_atoul(assoc);
				if (last_id != associd) {
					list_iterator_reset(a_itr);
					while ((a_usage = list_next(a_itr))) {
						if (a_usage->id == associd) {
							last_id = a_usage->id;
							break;
						}
					}
				}

				if (!a_usage) {
					a_usage = xmalloc(
						sizeof(local_id_usage_t));
					a_usage->id = associd;
					list_append(assoc_usage_list, a_usage);
					last_id = associd;
					a_usage->loc_assets = list_create(
						_destroy_local_asset_usage);
				}
				/* This only works with CPUs now. */
				_add_time_asset(a_usage->loc_assets,
						TIME_ALLOC, ASSET_CPU, seconds);
			}
			list_iterator_destroy(tmp_itr);
		}

		/* now apply the down time from the slurmctld disconnects */
		if (c_usage) {
			list_iterator_reset(c_itr);
			while ((loc_c_usage = list_next(c_itr))) {
				local_asset_usage_t *loc_asset;
				ListIterator tmp_itr = list_iterator_create(
					loc_c_usage->loc_assets);
				while ((loc_asset = list_next(tmp_itr)))
					_add_time_asset(c_usage->loc_assets,
							TIME_DOWN,
							loc_asset->id,
							loc_asset->total_time);
				list_iterator_destroy(tmp_itr);
			}

			if ((rc = _process_cluster_usage(
				     mysql_conn, cluster_name, curr_start,
				     curr_end, now, c_usage))
			    != SLURM_SUCCESS) {
				_destroy_local_cluster_usage(c_usage);
				goto end_it;
			}
		}

		list_iterator_reset(a_itr);
		while ((a_usage = list_next(a_itr)))
			_create_id_usage_insert(cluster_name, ASSOC_TABLES,
						curr_start, now,
						a_usage, &query);
		if (query) {
			if (debug_flags & DEBUG_FLAG_DB_USAGE)
				DB_DEBUG(mysql_conn->conn, "query\n%s", query);
			rc = mysql_db_query(mysql_conn, query);
			xfree(query);
			if (rc != SLURM_SUCCESS) {
				error("Couldn't add assoc hour rollup");
				_destroy_local_cluster_usage(c_usage);
				goto end_it;
			}
		}

		if (!track_wckey)
			goto end_loop;

		list_iterator_reset(w_itr);
		while ((w_usage = list_next(w_itr)))
			_create_id_usage_insert(cluster_name, WCKEY_TABLES,
						curr_start, now,
						w_usage, &query);
		if (query) {
			if (debug_flags & DEBUG_FLAG_DB_USAGE)
				DB_DEBUG(mysql_conn->conn, "query\n%s", query);
			rc = mysql_db_query(mysql_conn, query);
			xfree(query);
			if (rc != SLURM_SUCCESS) {
				error("Couldn't add wckey hour rollup");
				_destroy_local_cluster_usage(c_usage);
				goto end_it;
			}
		}

	end_loop:
		_destroy_local_cluster_usage(c_usage);
		list_flush(assoc_usage_list);
		list_flush(cluster_down_list);
		list_flush(wckey_usage_list);
		list_flush(resv_usage_list);
		curr_start = curr_end;
		curr_end = curr_start + add_sec;
	}
end_it:
	xfree(suspend_str);
	xfree(job_str);
	xfree(resv_str);
	list_iterator_destroy(a_itr);
	list_iterator_destroy(c_itr);
	list_iterator_destroy(w_itr);
	list_iterator_destroy(r_itr);

	FREE_NULL_LIST(assoc_usage_list);
	FREE_NULL_LIST(cluster_down_list);
	FREE_NULL_LIST(wckey_usage_list);
	FREE_NULL_LIST(resv_usage_list);

	assoc_mgr_unlock(&locks);

/* 	info("stop start %s", slurm_ctime(&curr_start)); */
/* 	info("stop end %s", slurm_ctime(&curr_end)); */

	/* go check to see if we archive and purge */

	if (rc == SLURM_SUCCESS)
		rc = _process_purge(mysql_conn, cluster_name, archive_data,
				    SLURMDB_PURGE_HOURS);

	return rc;
}
extern int as_mysql_nonhour_rollup(mysql_conn_t *mysql_conn,
				   bool run_month,
				   char *cluster_name,
				   time_t start, time_t end,
				   uint16_t archive_data)
{
	/* can't just add 86400 since daylight savings starts and ends every
	 * once in a while
	 */
	int rc = SLURM_SUCCESS;
	struct tm start_tm;
	time_t curr_start = start;
	time_t curr_end;
	time_t now = time(NULL);
	char *query = NULL;
	uint16_t track_wckey = slurm_get_track_wckey();
	char *unit_name;

	if (!localtime_r(&curr_start, &start_tm)) {
		error("Couldn't get localtime from start %ld", curr_start);
		return SLURM_ERROR;
	}

	start_tm.tm_sec = 0;
	start_tm.tm_min = 0;
	start_tm.tm_hour = 0;
	start_tm.tm_isdst = -1;

	if (run_month) {
		unit_name = "month";
		start_tm.tm_mday = 1;
		start_tm.tm_mon++;
	} else {
		unit_name = "day";
		start_tm.tm_mday++;
	}

	curr_end = mktime(&start_tm);

	while (curr_start < end) {
		if (debug_flags & DEBUG_FLAG_DB_USAGE)
			DB_DEBUG(mysql_conn->conn,
				 "curr %s is now %ld-%ld",
				 unit_name, curr_start, curr_end);
/* 		info("start %s", slurm_ctime(&curr_start)); */
/* 		info("end %s", slurm_ctime(&curr_end)); */
		query = xstrdup_printf(
			"insert into \"%s_%s\" (creation_time, mod_time, id, "
			"id_asset, time_start, alloc_secs) "
			"select %ld, %ld, id, id_asset, "
			"%ld, @ASUM:=SUM(alloc_secs) from \"%s_%s\" where "
			"(time_start < %ld && time_start >= %ld) "
			"group by id, id_asset on duplicate key update "
			"mod_time=%ld, alloc_secs=@ASUM;",
			cluster_name,
			run_month ? assoc_month_table : assoc_day_table,
			now, now, curr_start,
			cluster_name,
			run_month ? assoc_day_table : assoc_hour_table,
			curr_end, curr_start, now);
		/* We group on deleted here so if there are no entries
		   we don't get an error, just nothing is returned.
		   Else we get a bunch of NULL's
		*/
		xstrfmtcat(query,
			   "insert into \"%s_%s\" (creation_time, "
			   "mod_time, time_start, id_asset, count, "
			   "alloc_secs, down_secs, pdown_secs, "
			   "idle_secs, over_secs, resv_secs) "
			   "select %ld, %ld, "
			   "%ld, id_asset, @CPU:=MAX(count), "
			   "@ASUM:=SUM(alloc_secs), "
			   "@DSUM:=SUM(down_secs), "
			   "@PDSUM:=SUM(pdown_secs), "
			   "@ISUM:=SUM(idle_secs), "
			   "@OSUM:=SUM(over_secs), "
			   "@RSUM:=SUM(resv_secs) from \"%s_%s\" where "
			   "(time_start < %ld && time_start >= %ld) "
			   "group by deleted, id_asset "
			   "on duplicate key update "
			   "mod_time=%ld, count=@CPU, "
			   "alloc_secs=@ASUM, down_secs=@DSUM, "
			   "pdown_secs=@PDSUM, idle_secs=@ISUM, "
			   "over_secs=@OSUM, resv_secs=@RSUM;",
			   cluster_name,
			   run_month ? cluster_month_table : cluster_day_table,
			   now, now, curr_start,
			   cluster_name,
			   run_month ? cluster_day_table : cluster_hour_table,
			   curr_end, curr_start, now);
		if (track_wckey) {
			xstrfmtcat(query,
				   "insert into \"%s_%s\" (creation_time, "
				   "mod_time, id, id_asset, time_start, "
				   "alloc_secs) "
				   "select %ld, %ld, "
				   "id, id_asset, %ld, @ASUM:=SUM(alloc_secs) "
				   "from \"%s_%s\" where (time_start < %ld && "
				   "time_start >= %ld) group by id, id_asset "
				   "on duplicate key update "
				   "mod_time=%ld, alloc_secs=@ASUM;",
				   cluster_name,
				   run_month ? wckey_month_table :
				   wckey_day_table,
				   now, now, curr_start,
				   cluster_name,
				   run_month ? wckey_day_table :
				   wckey_hour_table,
				   curr_end, curr_start, now);
		}
		if (debug_flags & DEBUG_FLAG_DB_USAGE)
			DB_DEBUG(mysql_conn->conn, "query\n%s", query);
		rc = mysql_db_query(mysql_conn, query);
		xfree(query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add %s rollup", unit_name);
			return SLURM_ERROR;
		}

		curr_start = curr_end;
		if (!localtime_r(&curr_start, &start_tm)) {
			error("Couldn't get localtime from %s start %ld",
			      unit_name, curr_start);
			return SLURM_ERROR;
		}
		start_tm.tm_sec = 0;
		start_tm.tm_min = 0;
		start_tm.tm_hour = 0;
		start_tm.tm_mday++;
		start_tm.tm_isdst = -1;
		curr_end = mktime(&start_tm);
	}

/* 	info("stop start %s", slurm_ctime(&curr_start)); */
/* 	info("stop end %s", slurm_ctime(&curr_end)); */

	/* go check to see if we archive and purge */
	rc = _process_purge(mysql_conn, cluster_name, archive_data,
			    run_month ? SLURMDB_PURGE_MONTHS :
			    SLURMDB_PURGE_DAYS);
	return rc;
}
