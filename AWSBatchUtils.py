# run: python AWSBatchUtils_py

import json
import commands

def SubmitBatchJobs(
	batch_id,
	njobs,
	s3_path, 
	job_type = 'bootstrap-py-jobs', 
	job_id = '0', 
	task_ids = '0',
	job_name = 'boson-job', 
	job_queue = 'boson-job-queue', 
	job_definition = 'boson-batch-job', 
	region = 'us-west-2'):

	command = "aws batch submit-job" + \
				" --job-name " + job_name + \
				" --job-queue " + job_queue + \
				" --job-definition " + job_definition + \
				" --container-overrides '{\"command\":[\"sh\",\"driver_sh\"," + \
				"\"" + region + "\"," + \
				"\"" + str(batch_id) + "\"," + \
				"\"" + job_type + "\"," + \
				"\"" + str(njobs) + "\"," + \
				"\"" + str(job_id) + "\"," + \
				"\"" + task_ids + "\"," + \
				"\"" + s3_path + "\"]}'"
	# print command
	status, out = commands_getstatusoutput(command)

# # run 5 tasks
# print(SubmitBatchJobs(
# 	batch_id = 1,
# 	njobs = 4,
# 	s3_path = "s3://boson-base/pyboson-test/", 
# 	job_type = "run-py-tasks", 
# 	job_id = '0', 
# 	task_ids = '1,2,3,4,5',
# 	job_name = 'boson-job', 
# 	job_queue = 'boson-job-queue', 
# 	job_definition = 'boson-batch-job', 
# 	region = 'us-west-2'))

# # bootstrap jobs
# print(SubmitBatchJobs(
# 	batch_id = 1,
# 	njobs = 4,
# 	s3_path = 's3://boson-base/pyboson-test/',
# 	job_type = 'bootstrap-py-jobs',
# 	job_id = 0,
# 	task_ids = '0'))


#' Bootstrap jobs
#' 
#'  @param batch_id batch id; required
#'  @param ntasks number of tasks to solve; required
#'  @param njobs number of AWS Batch jobs to spawn for solving all parallel tasks; required
#'  @param s3_path path to an S3 folder; required
#'  @param job_name name of the Batch job; default value is 'boson-job'
#'  @param job_queue name of the job queue to use; default value is 'boson-job-queue'
#'  @param job_definition name of the job_definition; default value is 'boson-batch-job'
#'  @param region AWS region; default value is 'us-west-2'
def BootstrapBatchJobs (
  	batch_id,
  	ntasks,
  	njobs,
  	s3_path,
  	job_name = 'boson-job',
  	job_queue = 'boson-job-queue',
  	job_definition = 'boson-batch-job',
  	region = 'us-west-2'):

  	# partition tasks
	if (ntasks == 1) :
		task_partitions = [[1,],]
	else :
		tasks = np.arange(ntasks)
		task_partitions = np.split(tasks, njobs)

	out = {}
	job_idx = 0

	for t in task_partitions :
		job_idx += 1
		task_ids = ','.join(map(str, t))
		print 'Submitting tasks: {}'.format(task_ids)
		out[str(job_idx)] = SubmitBatchJobs(
			batch_id = batch_id,
			njobs = 1,
			s3_path = s3_path,
			job_type = 'run-py-tasks',
			job_id = str(job_idx),
			task_ids = task_ids)

	return(out)


#' Monitor status of AWS Batch jobs
#' 
#' @param job_ids vector of job-ids; required
#' @param ping frequency of printing job status in seconds; default is every 10 seconds
#' @param print_job_status level of details in printing job status; default value is 'summary'
def MonitorJobStatus (job_ids, print_job_status = c('summary', 'detailed', 'none')):
  out = system2('aws', c('batch', 'describe-jobs',
                         '--jobs', paste(job_ids, collapse = ' ')),
                stdout = TRUE
                )
  
  df = jsonlite::fromJSON(paste(out, collapse = ''))[[1]][, c('jobId', 'status')]
  if (print_job_status[1] == 'summary') {
    cat(paste0(format(Sys_time()), ' - '))
    tab = table(df$status)
    cat(paste(names(tab), tab, sep = ':')); cat('\n')
  } else if (print_job_status[1] == 'detailed') {
    print(format(Sys_time()))
    print(df)
  }
  
  return(df)


#' Create a AWS Batch Compute Environment
#' 
#' @param comp_env_name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
#' @param instance_types what type of EC2 instance to attach to the Compute Environment; default is 'm4_large'
#' @param min_vcpus minimum number of vcpus to maintain in the Compute Environment; default valus is 0
#' @param max_vcpus maximum number of vcpus to maintain in the Compute Environment; default valus is 2
#' @param initial_vcpus number of vcpus initially attached to the Compute Environment; default valus is 2
#' @param service_role_arn ARN of a role created in AWS IAM with the following policies attached: AmazonS3FullAccess, AWSBatchServiceRole, AWSBatchFullAccess; required
#' @param subnets subnets from AWS VPC; required
#' @param security_group_ids security_group_ids from AWS VPC; required
def CreateBatchComputeEnvironment (
  	comp_env_name = 'boson-comp-env',
  	instance_types = c("m4_large"),
  	min_vcpus = 0,
  	max_vcpus = 2,
  	initial_vcpus = 2,
  	service_role_arn,
  	subnets,
  	security_group_ids):

  system2('aws', c(
    'batch', 'create-compute-environment',
    '--compute-environment-name', comp_env_name,
    '--type', 'MANAGED',
    '--state', 'ENABLED',
    '--compute-resources', paste0(
      'type="EC2"',
      ',minvCpus=', min_vcpus,
      ',maxvCpus=', max_vcpus,
      ',desiredvCpus=', initial_vcpus,
      ',instanceTypes=', paste(instance_types,collapse = ','),
      ',subnets=', paste0(subnets, collapse = ','),
      ',securityGroupIds=', paste0(security_group_ids, collapse = ','),
      ',instanceRole="ecsInstanceRole"'
      ),
    '--service-role', service_role_arn
    )
  )

  # wait till up
  flag = TRUE
  while (flag) {
    out = system2('aws', c(
        'batch', 'describe-compute-environments',
        '--compute-environments', comp_env_name
      ),
      stdout = TRUE
    )
    # print(paste(out, collapse = ''))
    flag = ! (grepl(comp_env_name, paste(out, collapse = '')) && grepl('ENABLED', paste(out, collapse = '')))
    Sys_sleep(1)
  }


#' Delete a AWS Batch Compute Environment
#' 
#' @param comp_env_name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
def DeleteBatchComputeEnvironment (
  	comp_env_name = 'boson-comp-env'):
  # disable
  out = system2('aws', c(
    'batch', 'update-compute-environment',
    '--compute-environment', comp_env_name,
    '--state', 'DISABLED'
    ),
    stdout = T
  )

  # wait till disabled
  flag = TRUE
  while (flag) {
    out = system2('aws', c(
        'batch', 'describe-compute-environments',
        '--compute-environments', comp_env_name
      ),
      stdout = TRUE
    )
    # print(paste(out, collapse = ''))
    flag = grepl('ENABLED', paste(out, collapse = ''))
    Sys_sleep(1)
  }

  # delete
  system2('aws', c(
    'batch', 'delete-compute-environment',
    '--compute-environment', comp_env_name
    )
  )



#' Create a AWS Batch Job Queue
#' 
#' @param job_queue_name name of the AWS Job Queue; default is 'boson-job-queue'
#' @param comp_env_name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
def CreateJobQueue (
  	job_queue_name = 'boson-job-queue',
  	comp_env_name = 'boson-comp-env'):

  system2('aws', c(
    'batch', 'create-job-queue',
    '--job-queue-name', job_queue_name,
    '--state', 'ENABLED',
    '--priority', '1',
    '--compute-environment-order', paste0('order=1,computeEnvironment=',comp_env_name)
    )
  )

  # wait till up
  flag = TRUE
  while (flag) {
    out = system2('aws', c(
        'batch', 'describe-job-queues',
        '--job-queues', job_queue_name
      ),
      stdout = TRUE
    )
    # print(paste(out, collapse = ''))
    flag = ! (grepl(job_queue_name, paste(out, collapse = '')) && grepl('ENABLED', paste(out, collapse = '')))
    Sys_sleep(1)
  }


#' Delete a AWS Batch Job Queue
#' 
#' @param job_queue_name name of the AWS Job Queue; default is 'boson-job-queue'
def DeleteJobQueue (job_queue_name = 'boson-job-queue'):

  # disable
  system2('aws', c(
    'batch', 'update-job-queue',
    '--job-queue', job_queue_name,
    '--state', 'DISABLED'
    )
  )

  # wait till disabled
  flag = TRUE
  while (flag) {
    out = system2('aws', c(
        'batch', 'describe-job-queues',
        '--job-queues', job_queue_name
      ),
      stdout = TRUE
    )
    # print(paste(out, collapse = ''))
    flag = grepl('ENABLED', paste(out, collapse = ''))
    Sys_sleep(1)
  }

  # delete
  system2('aws', c(
    'batch', 'delete-job-queue',
    '--job-queue', job_queue_name
    )
  )


#' Register a AWS Batcj Job Definition
#' 
#' @param job_definition_name name if the AWS Job Definition; default is 'boson-job-definition'
#' @param vcpus number of vcpus to assign for solving job; default value is 1
#' @param memory memory in mb to assign for solving job; default value is 1024
def RegisterBosonJobDefinition (
  	job_definition_name = 'boson-job-definition',
  	vcpus = 1,
  	memory = 1024):

  system2('aws', c(
    'batch', 'register-job-definition',
    '--job-definition-name', job_definition_name,
    '--type','container',
    '--container-properties', paste0(
        '\'{"image": "757968107665_dkr_ecr_us-west-2_amazonaws_com/boson-docker-image:latest", "vcpus": ', vcpus,', "memory": ', memory,'}\''
      )
    )
  )


#' Deregister a AWS Batcj Job Definition
#' 
#' @param job_definition_name name if the AWS Job Definition; default is 'boson-job-definition'
def DeregisterBosonJobDefinition (
  	job_definition_name = 'boson-job-definition',
  	revision_id = 1):
  
  system2('aws', c(
    'batch', 'deregister-job-definition',
    '--job-definition', paste0(job_definition_name, ':', revision_id)
    )
  )