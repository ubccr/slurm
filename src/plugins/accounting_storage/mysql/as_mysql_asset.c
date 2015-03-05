/*****************************************************************************\
 *  as_mysql_asset.c - functions dealing with accounts.
 *****************************************************************************
 *
 *  Copyright (C) 2015 SchedMD LLC.
 *  Written by Danny Auble <da@schedmd.com>
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

#include "as_mysql_asset.h"
#include "as_mysql_usage.h"
#include "src/common/xstring.h"

extern int update_full_asset_query(void)
{
	ListIterator itr;
	slurmdb_asset_rec_t *asset_rec;

	xfree(asset_view_str);
	xfree(full_asset_query);
	/* This could probably be done a faster way since assets most
	 * likely won't change that much/often, but this only takes a
	 * small hit at the beginning of the slurmdbd or whenever a
	 * new asset is added.  If it becomes an issue we will look at
	 * only calling this when we know an asset has been
	 * added/removed. */

	xassert(assoc_mgr_asset_list);
	itr = list_iterator_create(assoc_mgr_asset_list);
	while ((asset_rec = list_next(itr))) {
		xstrfmtcat(asset_view_str,
			   ", max(if(id_asset=%u,count,NULL)) as ext_%u",
			   asset_rec->id, asset_rec->id);
		xstrfmtcat(full_asset_query, ", ext_%u", asset_rec->id);
	}
	list_iterator_destroy(itr);

	return SLURM_SUCCESS;
}

/* assoc_mgr_lock_t->asset write must be locked before calling this */
extern int update_asset_views(mysql_conn_t *mysql_conn, char *cluster_name)
{
	char *query, *event_ext, *job_ext;
	int rc = SLURM_SUCCESS;

	xassert(asset_view_str);
	xassert(full_asset_query);

	/* Create a view for easy access to the event_ext table	*/
	event_ext = xstrdup_printf(
		"drop view if exists \"%s_%s\";"
		"create view \"%s_%s\" as (select inx ext_inx %s "
		"from \"%s_%s\" group by inx);",
		cluster_name, event_ext_view,
		cluster_name, event_ext_view, asset_view_str,
		cluster_name, event_ext_table);

	if (debug_flags & DEBUG_FLAG_DB_ASSET)
		DB_DEBUG(mysql_conn->conn, "%s", event_ext);
	rc = mysql_db_query(mysql_conn, event_ext);
	xfree(event_ext);
	if (rc != SLURM_SUCCESS)
		error("problem altering event_ext");

	/* Create a view for easy access to the job_ext table */
	job_ext = xstrdup_printf(
		"drop view if exists \"%s_%s\";"
		"create view \"%s_%s\" as (select job_db_inx ext_job_db_inx %s "
		"from \"%s_%s\" group by job_db_inx);",
		cluster_name, job_ext_view,
		cluster_name, job_ext_view, asset_view_str,
		cluster_name, job_ext_table);

	if (debug_flags & DEBUG_FLAG_DB_ASSET)
		DB_DEBUG(mysql_conn->conn, "%s", job_ext);
	rc = mysql_db_query(mysql_conn, job_ext);
	xfree(job_ext);
	if (rc != SLURM_SUCCESS)
		error("problem altering job_ext");

	/* handle other views */

	query = xstrdup_printf(
		"drop view if exists \"%s_%s\";"
		"create view \"%s_%s\" as (select * from \"%s_%s\" t1 "
		"left join \"%s_%s\" t2 on t1.inx=t2.ext_inx);"
		"drop view if exists \"%s_%s\";"
		"create view \"%s_%s\" as (select * from \"%s_%s\" t1 "
		"left join \"%s_%s\" t2 on t1.job_db_inx=t2.ext_job_db_inx);",
		cluster_name, event_view,
		cluster_name, event_view,
		cluster_name, event_table,
		cluster_name, event_ext_view,
		cluster_name, job_view,
		cluster_name, job_view,
		cluster_name, job_table,
		cluster_name, job_ext_view);

	if (debug_flags & DEBUG_FLAG_DB_ASSET)
		DB_DEBUG(mysql_conn->conn, "%s", query);
	rc = mysql_db_query(mysql_conn, query);
	xfree(query);
	if (rc != SLURM_SUCCESS)
		error("problem altering job_ext");

	return rc;
}

extern int as_mysql_add_assets(mysql_conn_t *mysql_conn,
			       uint32_t uid, List asset_list)
{
	ListIterator itr = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_asset_rec_t *object = NULL;
	char *cols = NULL, *extra = NULL, *vals = NULL, *query = NULL,
		*tmp_extra = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int affect_rows = 0;
	char *cluster_name;
	assoc_mgr_lock_t locks = { WRITE_LOCK, NO_LOCK, NO_LOCK,
				   NO_LOCK, NO_LOCK, NO_LOCK, NO_LOCK };

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR))
		return ESLURM_ACCESS_DENIED;

	/* means just update the views */
	if (!asset_list)
		goto update_views;

	user_name = uid_to_string((uid_t) uid);
	itr = list_iterator_create(asset_list);
	while ((object = list_next(itr))) {
		if (!object->type || !object->type[0]) {
			error("We need a asset type.");
			rc = SLURM_ERROR;
			continue;
		} else if ((!strcasecmp(object->type, "gres") ||
			    !strcasecmp(object->type, "license"))) {
			if (!object->name) {
				error("%s type assets "
				      "need to have a name, "
				      "(i.e. Gres:GPU).  You gave none",
				      object->type);
				rc = SLURM_ERROR;
				continue;
			}
		} else /* only the above have a name */
			xfree(object->name);

		xstrcat(cols, "creation_time, type");
		xstrfmtcat(vals, "%ld, '%s'", now, object->type);
		xstrfmtcat(extra, "type='%s'", object->type);
		if (object->name) {
			xstrcat(cols, ", name");
			xstrfmtcat(vals, ", '%s'", object->name);
			xstrfmtcat(extra, ", name='%s'", object->name);
		}

		xstrfmtcat(query,
			   "insert into %s (%s) values (%s) "
			   "on duplicate key update deleted=0;",
			   asset_table, cols, vals);

		if (debug_flags & DEBUG_FLAG_DB_ASSET)
			DB_DEBUG(mysql_conn->conn, "query\n%s", query);
		object->id = mysql_db_insert_ret_id(mysql_conn, query);
		xfree(query);
		if (!object->id) {
			error("Couldn't add asset %s%s%s", object->type,
			      object->name ? ":" : "",
			      object->name ? object->name : "");
			xfree(cols);
			xfree(extra);
			xfree(vals);
			break;
		}

		affect_rows = last_affected_rows(mysql_conn);

		if (!affect_rows) {
			debug2("nothing changed %d", affect_rows);
			xfree(cols);
			xfree(extra);
			xfree(vals);
			continue;
		}

		tmp_extra = slurm_add_slash_to_quotes(extra);

		xstrfmtcat(query,
			   "insert into %s "
			   "(timestamp, action, name, actor, info, cluster) "
			   "values (%ld, %u, 'id=%d', '%s', '%s', '%s');",
			   txn_table,
			   now, DBD_ADD_ASSETS, object->id, user_name,
			   tmp_extra, mysql_conn->cluster_name);

		xfree(tmp_extra);
		xfree(cols);
		xfree(extra);
		xfree(vals);
		debug4("query\n%s", query);
		rc = mysql_db_query(mysql_conn, query);
		xfree(query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add txn");
		} else {
			if (addto_update_list(mysql_conn->update_list,
					      SLURMDB_ADD_ASSET,
					      object) == SLURM_SUCCESS)
				list_remove(itr);
		}

	}
	list_iterator_destroy(itr);
	xfree(user_name);

update_views:
	if (assoc_mgr_update(mysql_conn->update_list)) {
		/* We only want to update the local cache DBD or ctld */
		assoc_mgr_update(mysql_conn->update_list);
		list_flush(mysql_conn->update_list);
	}

	/* For some reason we are unable to update the views while
	   rollup is running so we have to wait for it to finish with
	   the usage_rollup_lock.
	*/
	slurm_mutex_lock(&usage_rollup_lock);
	slurm_mutex_lock(&as_mysql_cluster_list_lock);
	assoc_mgr_lock(&locks);
	update_full_asset_query();
	itr = list_iterator_create(as_mysql_total_cluster_list);
	while ((cluster_name = list_next(itr)))
		update_asset_views(mysql_conn, cluster_name);
	list_iterator_destroy(itr);
	assoc_mgr_unlock(&locks);
	slurm_mutex_unlock(&as_mysql_cluster_list_lock);
	slurm_mutex_unlock(&usage_rollup_lock);

	return rc;
}

extern List as_mysql_get_assets(mysql_conn_t *mysql_conn, uid_t uid,
				slurmdb_asset_cond_t *asset_cond)
{
	char *query = NULL;
	char *extra = NULL;
	char *tmp = NULL;
	List asset_list = NULL;
	ListIterator itr = NULL;
	char *object = NULL;
	int set = 0;
	int i=0;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	bool is_admin = false;

	/* if this changes you will need to edit the corresponding enum */
	char *asset_req_inx[] = {
		"id",
		"type",
		"name"
	};
	enum {
		SLURMDB_REQ_ID,
		SLURMDB_REQ_TYPE,
		SLURMDB_REQ_NAME,
		SLURMDB_REQ_COUNT
	};

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	if (!(is_admin = is_user_min_admin_level(
		      mysql_conn, uid, SLURMDB_ADMIN_OPERATOR))) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	if (!asset_cond) {
		xstrcat(extra, "where deleted=0");
		goto empty;
	}

	if (asset_cond->with_deleted)
		xstrcat(extra, "where (deleted=0 || deleted=1)");
	else
		xstrcat(extra, "where deleted=0");

	if (asset_cond->id_list
	    && list_count(asset_cond->id_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(asset_cond->id_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "id='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (asset_cond->type_list
	    && list_count(asset_cond->type_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(asset_cond->type_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "type='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

	if (asset_cond->name_list
	    && list_count(asset_cond->name_list)) {
		set = 0;
		xstrcat(extra, " && (");
		itr = list_iterator_create(asset_cond->name_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(extra, " || ");
			xstrfmtcat(extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(extra, ")");
	}

empty:

	xfree(tmp);
	xstrfmtcat(tmp, "%s", asset_req_inx[i]);
	for(i=1; i<SLURMDB_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", asset_req_inx[i]);
	}

	query = xstrdup_printf("select %s from %s %s", tmp, asset_table, extra);
	xfree(tmp);
	xfree(extra);

	if (debug_flags & DEBUG_FLAG_DB_ASSET)
		DB_DEBUG(mysql_conn->conn, "query\n%s", query);
	if (!(result = mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return NULL;
	}
	xfree(query);

	asset_list = list_create(slurmdb_destroy_asset_rec);

	while ((row = mysql_fetch_row(result))) {
		slurmdb_asset_rec_t *asset =
			xmalloc(sizeof(slurmdb_asset_rec_t));
		list_append(asset_list, asset);

		asset->id =  slurm_atoul(row[SLURMDB_REQ_ID]);
		if (row[SLURMDB_REQ_TYPE] && row[SLURMDB_REQ_TYPE][0])
			asset->type = xstrdup(row[SLURMDB_REQ_TYPE]);
		if (row[SLURMDB_REQ_NAME] && row[SLURMDB_REQ_NAME][0])
			asset->name = xstrdup(row[SLURMDB_REQ_NAME]);
	}
	mysql_free_result(result);

	return asset_list;
}
