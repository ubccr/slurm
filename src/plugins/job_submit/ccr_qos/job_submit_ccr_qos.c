#include <stdio.h>


#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>
#include "src/common/slurm_xlator.h"
#include "src/slurmctld/slurmctld.h"


const char plugin_name[]="CCR modify qos jobsubmit plugin";
const char plugin_type[]="job_submit/ccr_qos";

const uint32_t plugin_version   = SLURM_VERSION_NUMBER;

extern int job_submit(struct job_descriptor *job_desc, uint32_t submit_uid, char **err_msg) {

	if(job_desc->account){
	    if( strstr(job_desc->account, "pi-") ){
		/* Consolidate to one account without the "pi-" */
		char piacct[64];
		strncpy( piacct, job_desc->account, 64);
		piacct[63]='\0';
		char* nopi = piacct + 3;
		info("Removing PI account: %s, for User: %u. Changing to %s", job_desc->account, submit_uid, nopi);
		xfree(job_desc->account);
		xstrcat(job_desc->account, nopi);
	    }
	}

	if (job_desc->qos){
	    if ( strcmp( job_desc->qos, "supporters" ) == 0 ){
		/* Leave this job alone */
		return SLURM_SUCCESS;
	    }

	    /* Is it the correct QOS */
            if ( job_desc->partition && (strcmp( job_desc->qos, job_desc->partition) !=0 )){
                info("Clearing Bad QOS: %s for UID: %u on Partition: %s", job_desc->qos, submit_uid, job_desc->partition);
                xfree(job_desc->qos);
            }
	}

        /* Can't use "else" since we take off a bad QOS in the "if" */
        if(job_desc->partition && !job_desc->qos){
		/* Missing, add a qos that matches the partition name */
		xstrcat(job_desc->qos, job_desc->partition);
                info("Adding Missing QOS: %s for UID: %u on Partition: %s", job_desc->qos, submit_uid, job_desc->partition);
	}

        /* Not sure what happens if there is no partition or QOS */

        return SLURM_SUCCESS;
}

int job_modify(struct job_descriptor *job_desc, struct job_record *job_ptr,
               uint32_t submit_uid)
{
        return SLURM_SUCCESS;
}
