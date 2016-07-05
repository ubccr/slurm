#include <stdio.h>


#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>
#include "src/common/slurm_xlator.h"
#include "src/slurmctld/slurmctld.h"


const char plugin_name[]="CCR modify qos jobsubmit plugin";
const char plugin_type[]="job_submit/ccr_qos";
const uint32_t plugin_version   = 110;
const uint32_t min_plug_version = 100;

extern int job_submit(struct job_descriptor *job_desc, uint32_t submit_uid, char **err_msg) {

	if(job_desc->partition){
		if( strcmp( job_desc->partition, "supporters") == 0 ){
			info("User: %u submitted a job to supporters, moving to general compute and setting qos\n", submit_uid);
			
			// Delete the old partition and add the new one
			xfree(job_desc->partition);
			xstrcat(job_desc->partition, "general-compute");

			// Remove any existing QOS, shouldn't be any, but just to be safe
			if (job_desc->qos){
				info("Clearing existing QOS (%s)", job_desc->qos);
                		xfree(job_desc->qos);
			}

			// Add the new qos
			xstrcat(job_desc->qos, "supporters");
			info("Job now has qos: %s\n", job_desc->qos);
		}
	}

        return SLURM_SUCCESS;
}

int job_modify(struct job_descriptor *job_desc, struct job_record *job_ptr,
               uint32_t submit_uid)
{
        return SLURM_SUCCESS;
}
