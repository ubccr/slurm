#include <stdio.h>


#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>
#include "src/common/slurm_xlator.h"
#include "src/slurmctld/slurmctld.h"


const char plugin_name[]="CCR modify qos jobsubmit plugin";
const char plugin_type[]="job_submit/ccr_qos";

const uint32_t plugin_version   = SLURM_VERSION_NUMBER;

extern int job_submit(struct job_descriptor *job_desc, uint32_t submit_uid, char **err_msg) {

	if(job_desc->partition){
		// Add a qos that matches the partition name
		xstrcat(job_desc->qos, job_desc->partition);
	}

        return SLURM_SUCCESS;
}

int job_modify(struct job_descriptor *job_desc, struct job_record *job_ptr,
               uint32_t submit_uid)
{
        return SLURM_SUCCESS;
}
