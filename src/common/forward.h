/*****************************************************************************\
 *  forward.h - get/print the job state information of slurm
 *
 *  $Id$
 *****************************************************************************
 *  Copyright (C) 2006 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <auble1@llnl.gov>
 *  UCRL-CODE-217948.
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

#ifndef _FORWARD_H
#define _FORWARD_H

#include <stdint.h>
#include "src/common/slurm_protocol_api.h"
#include "src/common/dist_tasks.h"

/* STRUCTURES */
extern void forward_init(forward_t *forward, forward_t *from);

extern int forward_msg(forward_struct_t *forward_struct, 
		       header_t *header);

/*
 * set_forward_addrs - add to the message possible forwards to go to
 * IN: forward     - forward_t *   - struct to store forward info
 * IN: thr_count   - int           - number of messages already done
 * IN: from        - forward_t *   - info to separate into new forward struct
 * RET: SLURM_SUCCESS - int
 */
extern int forward_set (forward_t *forward, 
			int span,
			int *pos,
			forward_t *from);

extern int forward_set_launch (forward_t *forward, 
			       int span,
			       int *pos,
			       slurm_step_layout_t *step_layout,
			       slurm_addr *slurmd_addr,
			       hostlist_iterator_t itr,
			       int32_t timeout);

extern int no_resp_forwards(forward_t *forward, List *ret_list, int err);

/* destroyers */
extern void destroy_data_info(void *object);
extern void destroy_forward(forward_t *forward);
extern void destroy_forward_struct(forward_struct_t *forward_struct);
extern void destroy_ret_types(void *object);

#endif
