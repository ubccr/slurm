#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>

#include "src/slurmctld/slurmctld.h"

#include <stdio.h>

const char plugin_name[]="Save script jobsubmit plugin";
const char plugin_type[]="job_submit/save_script";
const uint32_t plugin_version   = SLURM_VERSION_NUMBER;

extern int job_submit(struct job_descriptor *job_desc, uint32_t submit_uid, char **err_msg) {

        if( job_desc->script != NULL){

		char tmpfile[] = "/projects/ccr/slurm/jobscripts/queued/jobscript-XXXXXX";

		int sts = mkstemp( tmpfile );

		if( sts < 0 ){
			// Just continue on.  Run the job, but script wont be saved
			return SLURM_SUCCESS;
		}

		// Make it readable by ccr staff
		fchmod( sts, 0640);

		FILE * outfile = fdopen( sts, "w" );
		fprintf(outfile, "%s", job_desc->script);
		fclose(outfile);

		// Send the location, instead of the script itself.
		// Something in the pipeline was doing shell expansion on the script

               	if( env_array_append (&(job_desc->spank_job_env), "SAVE_BATCH", tmpfile) ){
               		job_desc->spank_job_env_size++;
               	}
        }

        return SLURM_SUCCESS;
}

int job_modify(struct job_descriptor *job_desc, struct job_record *job_ptr,
               uint32_t submit_uid)
{
        return SLURM_SUCCESS;
}
