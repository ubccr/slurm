/*****************************************************************************\
 *  as_mysql_convert.c - functions dealing with converting from tables in
 *                    slurm <= 14.11.
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

#include "as_mysql_convert.h"

bool ext_tables_created = 0;

static int _rename_usage_columns(mysql_conn_t *mysql_conn, char *table)
{
	MYSQL_ROW row;
	MYSQL_RES *result = NULL;
	char *query = NULL;
	int rc = SLURM_SUCCESS;

	query = xstrdup_printf(
		"show columns from %s where field like '%%cpu_%%' "
		"|| field like 'id_assoc' || field like 'id_wckey';",
		table);

	debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
	result = mysql_db_query_ret(mysql_conn, query, 0);
	xfree(query);

	if (!result)
		return SLURM_ERROR;

	while ((row = mysql_fetch_row(result))) {
		char *new_char = xstrdup(row[0]);
		xstrsubstitute(new_char, "cpu_", "");
		xstrsubstitute(new_char, "_assoc", "");
		xstrsubstitute(new_char, "_wckey", "");

		if (!query)
			query = xstrdup_printf("alter table %s ", table);
		else
			xstrcat(query, ", ");

		if (!strcmp("id", new_char))
			xstrfmtcat(query, "change %s %s int unsigned not null",
				   row[0], new_char);
		else
			xstrfmtcat(query,
				   "change %s %s bigint unsigned default "
				   "0 not null",
				   row[0], new_char);
		xfree(new_char);
	}
	mysql_free_result(result);

	if (query) {
		debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
		if ((rc = mysql_db_query(mysql_conn, query)) != SLURM_SUCCESS)
			error("Can't update %s %m", table);
		xfree(query);
	}

	return rc;
}


static int _update_old_cluster_tables(mysql_conn_t *mysql_conn,
				      char *cluster_name)
{
	/* These tables are the 14_11 defs plus things we added in 15.08 */

	storage_field_t assoc_usage_table_fields_14_11[] = {
		{ "creation_time", "int unsigned not null" },
		{ "mod_time", "int unsigned default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "id_assoc", "int not null" },
		{ "time_start", "int unsigned not null" },
		{ "id_asset", "int default 1 not null" },
		{ "alloc_cpu_secs", "bigint default 0 not null" },
		{ "consumed_energy", "bigint unsigned default 0 not null" },
		{ NULL, NULL}
	};

	storage_field_t cluster_usage_table_fields_14_11[] = {
		{ "creation_time", "int unsigned not null" },
		{ "mod_time", "int unsigned default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "time_start", "int unsigned not null" },
		{ "id_asset", "int default 1 not null" },
		{ "cpu_count", "int default 0 not null" },
		{ "alloc_cpu_secs", "bigint default 0 not null" },
		{ "down_cpu_secs", "bigint default 0 not null" },
		{ "pdown_cpu_secs", "bigint default 0 not null" },
		{ "idle_cpu_secs", "bigint default 0 not null" },
		{ "resv_cpu_secs", "bigint default 0 not null" },
		{ "over_cpu_secs", "bigint default 0 not null" },
		{ "consumed_energy", "bigint unsigned default 0 not null" },
		{ NULL, NULL}
	};

	storage_field_t event_table_fields_14_11[] = {
		{ "time_start", "int unsigned not null" },
		{ "time_end", "int unsigned default 0 not null" },
		{ "inx", "int unsigned not null auto_increment" },
		{ "node_name", "tinytext default '' not null" },
		{ "cluster_nodes", "text not null default ''" },
		{ "cpu_count", "int not null" },
		{ "reason", "tinytext not null" },
		{ "reason_uid", "int unsigned default 0xfffffffe not null" },
		{ "state", "smallint unsigned default 0 not null" },
		{ NULL, NULL}
	};

	storage_field_t job_table_fields_14_11[] = {
		{ "job_db_inx", "int not null auto_increment" },
		{ "mod_time", "int unsigned default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "account", "tinytext" },
		{ "array_task_str", "text" },
		{ "array_max_tasks", "int unsigned default 0 not null" },
		{ "array_task_pending", "int unsigned default 0 not null" },
		{ "cpus_req", "int unsigned not null" },
		{ "cpus_alloc", "int unsigned not null" },
		{ "derived_ec", "int unsigned default 0 not null" },
		{ "derived_es", "text" },
		{ "exit_code", "int unsigned default 0 not null" },
		{ "job_name", "tinytext not null" },
		{ "id_assoc", "int unsigned not null" },
		{ "id_array_job", "int unsigned default 0 not null" },
		{ "id_array_task", "int unsigned default 0xfffffffe not null" },
		{ "id_block", "tinytext" },
		{ "id_job", "int unsigned not null" },
		{ "id_qos", "int unsigned default 0 not null" },
		{ "id_resv", "int unsigned not null" },
		{ "id_wckey", "int unsigned not null" },
		{ "id_user", "int unsigned not null" },
		{ "id_group", "int unsigned not null" },
		{ "kill_requid", "int default -1 not null" },
		{ "mem_req", "int unsigned default 0 not null" },
		{ "nodelist", "text" },
		{ "nodes_alloc", "int unsigned not null" },
		{ "node_inx", "text" },
		{ "partition", "tinytext not null" },
		{ "priority", "int unsigned not null" },
		{ "state", "smallint unsigned not null" },
		{ "timelimit", "int unsigned default 0 not null" },
		{ "time_submit", "int unsigned default 0 not null" },
		{ "time_eligible", "int unsigned default 0 not null" },
		{ "time_start", "int unsigned default 0 not null" },
		{ "time_end", "int unsigned default 0 not null" },
		{ "time_suspended", "int unsigned default 0 not null" },
		{ "gres_req", "text not null default ''" },
		{ "gres_alloc", "text not null default ''" },
		{ "gres_used", "text not null default ''" },
		{ "wckey", "tinytext not null default ''" },
		{ "track_steps", "tinyint not null" },
		{ NULL, NULL}
	};

	/* storage_field_t resv_table_fields_14_11[] = { */
	/* 	{ "inx", "int unsigned not null auto_increment" }, */
	/* 	{ "id_resv", "int unsigned default 0 not null" }, */
	/* 	{ "deleted", "tinyint default 0 not null" }, */
	/* 	{ "assoclist", "text not null default ''" }, */
	/* 	{ "cpus", "int unsigned not null" }, */
	/* 	{ "flags", "smallint unsigned default 0 not null" }, */
	/* 	{ "nodelist", "text not null default ''" }, */
	/* 	{ "node_inx", "text not null default ''" }, */
	/* 	{ "resv_name", "text not null" }, */
	/* 	{ "time_start", "int unsigned default 0 not null"}, */
	/* 	{ "time_end", "int unsigned default 0 not null" }, */
	/* 	{ NULL, NULL} */
	/* }; */

	/* storage_field_t step_table_fields_14_11[] = { */
	/* 	{ "job_db_inx", "int not null" }, */
	/* 	{ "deleted", "tinyint default 0 not null" }, */
	/* 	{ "cpus_alloc", "int unsigned not null" }, */
	/* 	{ "exit_code", "int default 0 not null" }, */
	/* 	{ "id_step", "int not null" }, */
	/* 	{ "kill_requid", "int default -1 not null" }, */
	/* 	{ "nodelist", "text not null" }, */
	/* 	{ "nodes_alloc", "int unsigned not null" }, */
	/* 	{ "node_inx", "text" }, */
	/* 	{ "state", "smallint unsigned not null" }, */
	/* 	{ "step_name", "text not null" }, */
	/* 	{ "task_cnt", "int unsigned not null" }, */
	/* 	{ "task_dist", "smallint default 0 not null" }, */
	/* 	{ "time_start", "int unsigned default 0 not null" }, */
	/* 	{ "time_end", "int unsigned default 0 not null" }, */
	/* 	{ "time_suspended", "int unsigned default 0 not null" }, */
	/* 	{ "user_sec", "int unsigned default 0 not null" }, */
	/* 	{ "user_usec", "int unsigned default 0 not null" }, */
	/* 	{ "sys_sec", "int unsigned default 0 not null" }, */
	/* 	{ "sys_usec", "int unsigned default 0 not null" }, */
	/* 	{ "max_pages", "int unsigned default 0 not null" }, */
	/* 	{ "max_pages_task", "int unsigned default 0 not null" }, */
	/* 	{ "max_pages_node", "int unsigned default 0 not null" }, */
	/* 	{ "ave_pages", "double unsigned default 0.0 not null" }, */
	/* 	{ "max_rss", "bigint unsigned default 0 not null" }, */
	/* 	{ "max_rss_task", "int unsigned default 0 not null" }, */
	/* 	{ "max_rss_node", "int unsigned default 0 not null" }, */
	/* 	{ "ave_rss", "double unsigned default 0.0 not null" }, */
	/* 	{ "max_vsize", "bigint unsigned default 0 not null" }, */
	/* 	{ "max_vsize_task", "int unsigned default 0 not null" }, */
	/* 	{ "max_vsize_node", "int unsigned default 0 not null" }, */
	/* 	{ "ave_vsize", "double unsigned default 0.0 not null" }, */
	/* 	{ "min_cpu", "int unsigned default 0xfffffffe not null" }, */
	/* 	{ "min_cpu_task", "int unsigned default 0 not null" }, */
	/* 	{ "min_cpu_node", "int unsigned default 0 not null" }, */
	/* 	{ "ave_cpu", "double unsigned default 0.0 not null" }, */
	/* 	{ "act_cpufreq", "double unsigned default 0.0 not null" }, */
	/* 	{ "consumed_energy", "double unsigned default 0.0 not null" }, */
	/* 	{ "req_cpufreq", "int unsigned default 0 not null" }, */
	/* 	{ "max_disk_read", "double unsigned default 0.0 not null" }, */
	/* 	{ "max_disk_read_task", "int unsigned default 0 not null" }, */
	/* 	{ "max_disk_read_node", "int unsigned default 0 not null" }, */
	/* 	{ "ave_disk_read", "double unsigned default 0.0 not null" }, */
	/* 	{ "max_disk_write", "double unsigned default 0.0 not null" }, */
	/* 	{ "max_disk_write_task", "int unsigned default 0 not null" }, */
	/* 	{ "max_disk_write_node", "int unsigned default 0 not null" }, */
	/* 	{ "ave_disk_write", "double unsigned default 0.0 not null" }, */
	/* 	{ NULL, NULL} */
	/* }; */

	storage_field_t wckey_usage_table_fields_14_11[] = {
		{ "creation_time", "int unsigned not null" },
		{ "mod_time", "int unsigned default 0 not null" },
		{ "deleted", "tinyint default 0 not null" },
		{ "id_wckey", "int not null" },
		{ "time_start", "int unsigned not null" },
		{ "id_asset", "int default 1 not null" },
		{ "alloc_cpu_secs", "bigint default 0" },
		{ "resv_cpu_secs", "bigint default 0" },
		{ "over_cpu_secs", "bigint default 0" },
		{ "consumed_energy", "bigint unsigned default 0 not null" },
		{ NULL, NULL}
	};

	char table_name[200];

	xassert(cluster_name);

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, assoc_day_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  assoc_usage_table_fields_14_11,
				  ", primary key (id_assoc, "
				  "time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, assoc_hour_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  assoc_usage_table_fields_14_11,
				  ", primary key (id_assoc, "
				  "time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, assoc_month_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  assoc_usage_table_fields_14_11,
				  ", primary key (id_assoc, "
				  "time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, cluster_day_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  cluster_usage_table_fields_14_11,
				  ", primary key (time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, cluster_hour_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  cluster_usage_table_fields_14_11,
				  ", primary key (time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, cluster_month_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  cluster_usage_table_fields_14_11,
				  ", primary key (time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, event_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  event_table_fields_14_11,
				  ", primary key (inx), index(node_name(20), "
				  "time_start))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, job_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  job_table_fields_14_11,
				  ", primary key (job_db_inx), "
				  "unique index (id_job, "
				  "id_assoc, time_submit), "
				  "key rollup (time_eligible, time_end), "
				  "key wckey (id_wckey), "
				  "key qos (id_qos), "
				  "key association (id_assoc), "
				  "key array_job (id_array_job), "
				  "key reserv (id_resv), "
				  "key sacct_def (id_user, time_start, "
				  "time_end))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	/* snprintf(table_name, sizeof(table_name), "\"%s_%s\"", */
	/* 	 cluster_name, resv_table); */
	/* if (mysql_db_create_table(mysql_conn, table_name, */
	/* 			  resv_table_fields_14_11, */
	/* 			  ", primary key (id_resv, time_start))") */
	/*     == SLURM_ERROR) */
	/* 	return SLURM_ERROR; */

	/* snprintf(table_name, sizeof(table_name), "\"%s_%s\"", */
	/* 	 cluster_name, step_table); */
	/* if (mysql_db_create_table(mysql_conn, table_name, */
	/* 			  step_table_fields_14_11, */
	/* 			  ", primary key (job_db_inx, id_step))") */
	/*     == SLURM_ERROR) */
	/* 	return SLURM_ERROR; */

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, wckey_day_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  wckey_usage_table_fields_14_11,
				  ", primary key (id_wckey, "
				  "time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, wckey_hour_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  wckey_usage_table_fields_14_11,
				  ", primary key (id_wckey, "
				  "time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	snprintf(table_name, sizeof(table_name), "\"%s_%s\"",
		 cluster_name, wckey_month_table);
	if (mysql_db_create_table(mysql_conn, table_name,
				  wckey_usage_table_fields_14_11,
				  ", primary key (id_wckey, "
				  "time_start, id_asset))")
	    == SLURM_ERROR)
		return SLURM_ERROR;

	return SLURM_SUCCESS;
}

static int _convert_event_table(mysql_conn_t *mysql_conn, char *cluster_name)
{
	MYSQL_ROW row;
	MYSQL_RES *result = NULL;
	char *query = NULL, *tmp = NULL;
	int rc = SLURM_SUCCESS, i;

	/* if this changes you will need to edit the corresponding enum */
	char *event_req_inx[] = {
		"inx",
		"cpu_count",
	};

	enum {
		REQ_INX,
		REQ_CPU,
		REQ_COUNT
	};
	int count = 0;

	xfree(tmp);
	xstrfmtcat(tmp, "%s", event_req_inx[0]);
	for (i=1; i<REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", event_req_inx[i]);
	}

	/* see if the clus_info table has been made */
	query = xstrdup_printf("select %s from \"%s_%s\"",
			       tmp, cluster_name, event_table);
	xfree(tmp);

	debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
	if (!(result = mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);

	while ((row = mysql_fetch_row(result))) {
		if (query) {
			xstrfmtcat(query, ", (%s, %d, %s)",
				   row[REQ_INX], ASSET_CPU, row[REQ_CPU]);
		} else {
			query = xstrdup_printf("insert into \"%s_%s\" "
					       "(inx, id_asset, count) values "
					       "(%s, %d, %s)",
					       cluster_name, event_ext_table,
					       row[REQ_INX], ASSET_CPU,
					       row[REQ_CPU]);
		}
		count++;
		if (count > 1000) {
			xstrfmtcat(query,
				   " on duplicate key update "
				   "count=VALUES(count);");

			debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
			if ((rc = mysql_db_query(mysql_conn, query))
			    != SLURM_SUCCESS) {
				xfree(query);
				error("Can't update %s event_table: %m",
				      cluster_name);
				break;
			}
			xfree(query);
			count = 0;
		}
	}
	mysql_free_result(result);

	if (query) {
		xstrfmtcat(query,
			   " on duplicate key update count=VALUES(count);");

		debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
		if ((rc = mysql_db_query(mysql_conn, query)) != SLURM_SUCCESS)
			error("Can't update %s event_table: %m", cluster_name);
		xfree(query);
	}

	return rc;
}

static int _convert_cluster_usage_table(mysql_conn_t *mysql_conn,
					char *table, char *table_ext)
{
	MYSQL_RES *result = NULL;
	MYSQL_ROW row = NULL;
	char *query = NULL, *tmp = NULL, *query2 = NULL;
	int rc = SLURM_SUCCESS, i;

	/* if this changes you will need to edit the corresponding enum */
	char *req_inx[] = {
		"alloc_cpu_secs",
		"down_cpu_secs",
		"pdown_cpu_secs",
		"idle_cpu_secs",
		"resv_cpu_secs",
		"over_cpu_secs",
		"cpu_count",
		"time_start",
		"consumed_energy"
	};

	enum {
		REQ_ACPU,
		REQ_DCPU,
		REQ_PDCPU,
		REQ_ICPU,
		REQ_RCPU,
		REQ_OCPU,
		REQ_CNT,
		REQ_START,
		REQ_ENERGY,
		REQ_COUNT
	};

	/* this needs to line up directly with the beginning of req_inx */
	char *ins_inx[] = {
		"alloc_secs",
		"down_secs",
		"pdown_secs",
		"idle_secs",
		"resv_secs",
		"over_secs",
		"count",
	};

	enum {
		INS_ACPU,
		INS_DCPU,
		INS_PDCPU,
		INS_ICPU,
		INS_RCPU,
		INS_OCPU,
		INS_CNT,
		INS_COUNT
	};
	int count = 0;

	xfree(tmp);
	xstrfmtcat(tmp, "%s", req_inx[0]);
	for (i=1; i<REQ_COUNT; i++)
		xstrfmtcat(tmp, ", %s", req_inx[i]);

	/* see if the clus_info table has been made */
	query = xstrdup_printf("select %s from %s", tmp, table);
	xfree(tmp);

	debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
	if (!(result = mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);

	xstrfmtcat(tmp, "%s", ins_inx[0]);
	for (i=1; i<INS_COUNT; i++)
		xstrfmtcat(tmp, ", %s", ins_inx[i]);

	while ((row = mysql_fetch_row(result))) {
		if (!query) {
			query = xstrdup_printf("insert into %s "
					       "(time_start, id_asset, %s) "
					       "values ",
					       table_ext, tmp);

			/* query2 is for energy which only has alloc_secs */
			query2 = xstrdup_printf("insert into %s "
						"(time_start, id_asset, "
						"alloc_secs) values ",
						table_ext);
		} else {
			xstrcat(query, ", ");
			xstrcat(query2, ", ");
		}

		xstrfmtcat(query, "(%s, %d", row[REQ_START], ASSET_CPU);
		xstrfmtcat(query2, "(%s, %d, %s)",
			   row[REQ_START], ASSET_ENERGY, row[REQ_ENERGY]);

		for (i=0; i<INS_COUNT; i++)
			xstrfmtcat(query, ", %s", row[i]);
		xstrcat(query, ")");
		count++;
		if (count > 1000) {
			/* break it up just to make sure we don't
			   break any buffer.
			*/
			xstrcat(query,
				" on duplicate key update "
				"count=VALUES(count), "
				"alloc_secs=VALUES(alloc_secs), "
				"down_secs=VALUES(down_secs), "
				"pdown_secs=VALUES(pdown_secs), "
				"idle_secs=VALUES(idle_secs), "
				"over_secs=VALUES(over_secs), "
				"resv_secs=VALUES(resv_secs);");
			xstrcat(query2,
				" on duplicate key update "
				"alloc_secs=VALUES(alloc_secs);");

			debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
			if ((rc = mysql_db_query(mysql_conn, query))
			    != SLURM_SUCCESS) {
				error("Can't convert %s info to %s: %m",
				      table, table_ext);
				xfree(query);
				xfree(query2);
				break;
			}
			xfree(query);
			debug4("(%s:%d) query\n%s",
			       THIS_FILE, __LINE__, query2);
			if ((rc = mysql_db_query(mysql_conn, query2))
			    != SLURM_SUCCESS) {
				error("Can't convert %s info to %s: %m",
				      table, table_ext);
				xfree(query2);
				break;
			}
			xfree(query2);
			count = 0;
		}
	}
	mysql_free_result(result);
	xfree(tmp);

	if (query) {
		xstrcat(query,
			" on duplicate key update count=VALUES(count), "
			"alloc_secs=VALUES(alloc_secs), "
			"down_secs=VALUES(down_secs), "
			"pdown_secs=VALUES(pdown_secs), "
			"idle_secs=VALUES(idle_secs), "
			"over_secs=VALUES(over_secs), "
			"resv_secs=VALUES(resv_secs);");
		xstrcat(query2,
			" on duplicate key update count=VALUES(count), "
			"alloc_secs=VALUES(alloc_secs);");
		debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
		if ((rc = mysql_db_query(mysql_conn, query)) != SLURM_SUCCESS)
			error("Can't convert %s info to %s: %m",
			      table, table_ext);
		xfree(query);
		debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query2);
		if ((rc = mysql_db_query(mysql_conn, query2)) != SLURM_SUCCESS)
			error("Can't convert %s info to %s: %m",
			      table, table_ext);
		xfree(query2);
	}

	return rc;
}

static int _convert_id_usage_table(mysql_conn_t *mysql_conn, char *table)
{
	char *query = NULL;
	int rc;

	if ((rc = _rename_usage_columns(mysql_conn, table)) != SLURM_SUCCESS)
		return rc;

	/* alter usage table (NOTE: this appears to be slower than
	 * the commented code, but this is cleaner and the code only
	 * happens once when converting so it isn't that big a deal,
	 * I opted for simple code instead of perhaps slightly
	 * faster.) */
	query = xstrdup_printf("insert into %s (creation_time, mod_time, "
			       "deleted, id, time_start, id_asset, alloc_secs) "
			       "select creation_time, mod_time, deleted, id, "
			       "time_start, %d, consumed_energy from %s where "
			       "consumed_energy != 0 on duplicate key update "
			       "mod_time=%ld, alloc_secs=VALUES(alloc_secs);",
			       table, ASSET_ENERGY, table, time(NULL));
	debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
	if ((rc = mysql_db_query(mysql_conn, query)) != SLURM_SUCCESS)
		error("Can't convert %s info: %m", table);
	xfree(query);

	return rc;
}

static int _convert_cluster_usage_tables(mysql_conn_t *mysql_conn,
					 char *cluster_name)
{
	char table[200], table_ext[200];
	int rc;

	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, cluster_day_table);
	snprintf(table_ext, sizeof(table_ext), "\"%s_%s\"",
		 cluster_name, cluster_day_ext_table);
	if ((rc = _convert_cluster_usage_table(mysql_conn, table, table_ext))
	    != SLURM_SUCCESS)
		return rc;

	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, cluster_hour_table);
	snprintf(table_ext, sizeof(table_ext), "\"%s_%s\"",
		 cluster_name, cluster_hour_ext_table);
	if ((rc = _convert_cluster_usage_table(mysql_conn, table, table_ext))
	    != SLURM_SUCCESS)
		return rc;

	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, cluster_month_table);
	snprintf(table_ext, sizeof(table_ext), "\"%s_%s\"",
		 cluster_name, cluster_month_ext_table);
	if ((rc = _convert_cluster_usage_table(mysql_conn, table, table_ext))
	    != SLURM_SUCCESS)
		return rc;

	/* assoc tables */
	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, assoc_day_table);
	if ((rc = _convert_id_usage_table(mysql_conn, table))
	    != SLURM_SUCCESS)
		return rc;

	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, assoc_hour_table);
	if ((rc = _convert_id_usage_table(mysql_conn, table))
	    != SLURM_SUCCESS)
		return rc;

	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, assoc_month_table);
	if ((rc = _convert_id_usage_table(mysql_conn, table))
	    != SLURM_SUCCESS)
		return rc;

	/* wckey tables */
	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, wckey_day_table);
	if ((rc = _convert_id_usage_table(mysql_conn, table))
	    != SLURM_SUCCESS)
		return rc;

	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, wckey_hour_table);
	if ((rc = _convert_id_usage_table(mysql_conn, table))
	    != SLURM_SUCCESS)
		return rc;

	snprintf(table, sizeof(table), "\"%s_%s\"",
		 cluster_name, wckey_month_table);
	if ((rc = _convert_id_usage_table(mysql_conn, table))
	    != SLURM_SUCCESS)
		return rc;

	return rc;
}

static int _convert_job_table(mysql_conn_t *mysql_conn, char *cluster_name)
{
	MYSQL_ROW row;
	MYSQL_RES *result = NULL;
	char *query = NULL, *tmp = NULL;
	int rc = SLURM_SUCCESS, i;

	/* if this changes you will need to edit the corresponding enum */
	char *event_req_inx[] = {
		"job_db_inx",
		"cpus_alloc",
		"mem_req",
	};

	enum {
		REQ_INX,
		REQ_CPU,
		REQ_MEM,
		REQ_COUNT
	};
	int count = 0;

	xfree(tmp);
	xstrfmtcat(tmp, "%s", event_req_inx[0]);
	for (i=1; i<REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", event_req_inx[i]);
	}

	/* see if the clus_info table has been made */
	query = xstrdup_printf("select %s from \"%s_%s\"",
			       tmp, cluster_name, job_table);
	xfree(tmp);

	debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
	if (!(result = mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);

	while ((row = mysql_fetch_row(result))) {
		if (query) {
			xstrfmtcat(query, ", (%s, %d, %s), (%s, %d, %s)",
				   row[REQ_INX], ASSET_CPU, row[REQ_CPU],
				   row[REQ_INX], ASSET_MEM, row[REQ_MEM]);
		} else {
			query = xstrdup_printf("insert into \"%s_%s\" "
					       "(job_db_inx, id_asset, count) "
					       "values (%s, %d, %s), "
					       "(%s, %d, %s)",
					       cluster_name, job_ext_table,
					       row[REQ_INX], ASSET_CPU,
					       row[REQ_CPU],
					       row[REQ_INX], ASSET_MEM,
					       row[REQ_MEM]);
		}
		count++;
		if (count > 1000) {
			xstrfmtcat(query,
				   " on duplicate key update "
				   "count=VALUES(count);");

			debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
			if ((rc = mysql_db_query(mysql_conn, query))
			    != SLURM_SUCCESS) {
				xfree(query);
				error("Can't update %s job_table: %m",
				      cluster_name);
				break;
			}
			xfree(query);
			count = 0;
		}
	}
	mysql_free_result(result);

	if (query) {
		xstrfmtcat(query,
			   " on duplicate key update count=VALUES(count);");

		debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
		if ((rc = mysql_db_query(mysql_conn, query)) != SLURM_SUCCESS)
			error("Can't update %s job_table: %m", cluster_name);
		xfree(query);
	}

	return rc;
}

extern int as_mysql_convert_tables(mysql_conn_t *mysql_conn)
{
	char *query;
	MYSQL_RES *result = NULL;
	int i = 0, rc = SLURM_SUCCESS;
	ListIterator itr;
	char *cluster_name;

	xassert(as_mysql_total_cluster_list);

	/* no valid clusters, just return */
	if (!(cluster_name = list_peek(as_mysql_total_cluster_list)))
		return SLURM_SUCCESS;

	/* See if the old table exist first.  If already ran here
	   default_acct and default_wckey won't exist.
	*/
	query = xstrdup_printf("show columns from \"%s_%s\" where "
			       "Field='cpu_count';",
			       cluster_name, event_table);

	debug4("(%s:%d) query\n%s", THIS_FILE, __LINE__, query);
	if (!(result = mysql_db_query_ret(mysql_conn, query, 0))) {
		xfree(query);
		return SLURM_ERROR;
	}
	xfree(query);
	i = mysql_num_rows(result);
	mysql_free_result(result);
	result = NULL;

	if (!i) {
		debug2("It appears the table conversions have already "
		       "taken place, hooray!");
		return SLURM_SUCCESS;
	}

	info("Updating database tables, this may take some time, "
	     "do not stop the process.");

	/* make it up to date */
	itr = list_iterator_create(as_mysql_total_cluster_list);
	while ((cluster_name = list_next(itr))) {
		/* make sure old tables are up to date */
		if (_update_old_cluster_tables(mysql_conn, cluster_name)
		    != SLURM_SUCCESS)
			break;

		/* create new tables needed for conversion */
		if (create_cluster_ext_tables(mysql_conn, cluster_name)
		    != SLURM_SUCCESS)
			break;

		/* Convert the event table first */
		info("converting event table for %s", cluster_name);
		if (_convert_event_table(mysql_conn, cluster_name)
		    != SLURM_SUCCESS)
			break;

		/* Now convert the cluster usage tables */
		info("converting cluster usage tables for %s", cluster_name);
		if (_convert_cluster_usage_tables(mysql_conn, cluster_name)
		    != SLURM_SUCCESS)
			break;

		/* Now convert the job tables */
		info("converting job table for %s", cluster_name);
		if (_convert_job_table(mysql_conn, cluster_name)
		    != SLURM_SUCCESS)
			break;
	}
	list_iterator_destroy(itr);

	if (rc == SLURM_SUCCESS) {
		info("Conversion done: success!");
		ext_tables_created = 1;
	}

	return rc;
}
