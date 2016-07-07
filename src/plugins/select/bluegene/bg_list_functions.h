/*****************************************************************************\
 *  bg_list_functions.c - header for dealing with the lists that
 *                        contain bg_records.
 *****************************************************************************
 *  Copyright (C) 2011 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
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

#ifndef _BRIDGE_BG_LIST_FUNCTIONS_H_
#define _BRIDGE_BG_LIST_FUNCTIONS_H_

#include "src/common/read_config.h"
#include "src/common/parse_spec.h"
#include "src/slurmctld/proc_req.h"
#include "src/common/list.h"
#include "src/common/hostlist.h"
#include "src/common/bitstring.h"
#include "src/common/xstring.h"
#include "src/common/xmalloc.h"
#include "bg_structs.h"

/* see if the exact record already exists in a list */
extern bg_record_t *block_exist_in_list(List my_list, bg_record_t *bg_record);
extern int block_ptr_exist_in_list(List my_list, bg_record_t *bg_record);
extern bg_record_t *find_bg_record_in_list(List my_list,
					   const char *bg_block_id);
extern int remove_from_bg_list(List my_list, bg_record_t *bg_record);
extern bg_record_t *find_and_remove_org_from_bg_list(List my_list,
						     bg_record_t *bg_record);
extern bg_record_t *find_org_in_bg_list(List my_list, bg_record_t *bg_record);
extern struct job_record *find_job_in_bg_record(bg_record_t *bg_record,
						uint32_t job_id);

#endif
