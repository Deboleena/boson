# run: python AWSBatchUtils_py

import json
import commands
import datetime
import re
import numpy as np
import time 

#' Submit a job to either bootstrap more jobs or solve tasks
#' 
#'  @param batch.id batch id; required
#'  @param job_type job type, can be 'bootstrap-py-jobs' or 'run-py-tasks'; default value is 'bootstrap-py-jobs'
#'  @param njobs if job_type = 'bootstrap-py-jobs', number of AWS Batch jobs to spawn for solving all parallel tasks; required
#'  @param s3_path path to an S3 folder; required
#'  @param job_id job_type = 'run-py-tasks', the job id; default value is '0'
#'  @param task_ids job_type = 'run-py-tasks', the task ids to solve as one job; default value is '0'
#'  @param job_name name of the Batch job; default value is 'boson-job'
#'  @param job_queue name of the job queue to use; default value is 'boson-job-queue'
#'  @param job_definition name of the job.definition; default value is 'boson-batch-job'
#'  @param region AWS region; default value is 'us-west-2'

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
def BootstrapBatchJobs(
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

def MonitorJobStatus(job_ids, print_job_status = ('summary', 'detailed', 'none')):
	command = "aws batch describe-jobs --jobs {}".format(" ".join(job_ids))
	status, output = commands.getstatusoutput(command)

	d = {}
	for i in out.split(" "):
    	k,v = i.split(" ")
    	d.setdefault(k,[]).append(v)
	
	jsonarray = json.dumps(d)
	obj1 = json.loads(jsonarray)
	df = pd.DataFrame(obj1, index=['jobId', 'status'])
	if print_job_status[1] == 'summary':
		tab = df['status']
  	else if (print_job_status[1] == 'detailed'):
  		now = datetime.datetime.now()
    	print now
    	print df 

    return df

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

def CreateBatchComputeEnvironment(
  	comp_env_name = 'boson-comp-env',
  	instance_types = c("m4_large"),
  	min_vcpus = 0,
  	max_vcpus = 2,
  	initial_vcpus = 2,
  	service_role_arn,
  	subnets,
  	security_group_ids):

  	command1 = "aws batch create-compute-environment --compute-environment-name{}".format(comp_env_name)
   		+ "--type MANAGED "
   		+ "--state ENABLED"
  		+ " --compute-resources"
      	+ 'type="EC2"'
      	+ 'minvCpus={}'.format(min_vcpus)
      	+ 'maxvCpus={}'.format(max_vcpus)
      	+ 'desiredvCpus={}'.format(initial_vcpus)
      	+ 'instanceTypes={}'.format(", ".join(instance_types))
        + 'subnets={}'.format(", ".join(subnets))
        + 'securityGroupIds={}'.format(", ".join(security_group_ids))
        + 'instanceRole="ecsInstanceRole"'
        + '--service-role {}'.format(service_role_arn)

    os.system(command1)
      
    # wait till up
    flag = True
    while flag :
    	command2 = "aws batch describe-compute-environments {}".format(comp_env_name)
    	status, out = commands_getstatusoutput(command2)

    	flag = ! bool(re.search(comp_env_name, out)) && bool(re.search(r"ENABLED", out))
    	time.sleep(1)

#' Delete a AWS Batch Compute Environment
#' 
#' @param comp_env_name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
def DeleteBatchComputeEnvironment(comp_env_name = 'boson-comp-env'):
	command1 = "aws batch describe-compute-environments --compute-environments {}".format(comp_env_name)
	status, out = commands_getstatusoutput(command1)

	flag = bool(re.search(r"ENABLED", out))
	time.sleep(1)

	command2 = "aws batch delete-compute-environment --compute-environment {}".format(comp_env_name)
	status, out = commands_getstatusoutput(command2)

#' Create a AWS Batch Job Queue
#' 
#' @param job_queue_name name of the AWS Job Queue; default is 'boson-job-queue'
#' @param comp_env_name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
def CreateJobQueue(job_queue_name = 'boson-job-queue', comp_env_name = 'boson-comp-env'):
	command1 = "aws batch create-job-queue --job-queue-name {}" .format(job_queue_name)
		+ "--state ENABLED"
		+ "--priority 1"
		+ "--compute-environment-order 1"
		+ "computeEnvironment= {}".format(comp_env_name)
	os.system(command1)

	# wait till up
	flag = True
  	while flag :
  		command2 = "aws batch describe-job-queues --job-queues {}".format(job_queue_name)
      	status, out = commands_getstatusoutput(command2)
   
    	flag = ! bool(re.search(job_queue_name, out)) && bool(re.search(r"ENABLED", out))
    	time.sleep(1)
  
#' Delete a AWS Batch Job Queue
#' 
#' @param job_queue_name name of the AWS Job Queue; default is 'boson-job-queue'
def DeleteJobQueue (job_queue_name = 'boson-job-queue'):
	# disable
	command1 = "aws batch update-job-queue --job-queue {} --state DISABLED ".format(job_queue_name)
    os.system(command1)

    # wait till disabled
    flag = True
  	while flag :
  		command2 = "aws batch describe-job-queues --job-queues {}".format(job_queue_name)
  		status, out = commands_getstatusoutput(command2)
    	flag = bool(re.search(r"ENABLED", out))
    	time.sleep(1)

    # delete
  	command3 = "aws batch delete-job-queue --job-queue {}".format(job_queue_name)
 	os.system(command3)

#' Register a AWS Batch Job Definition
#' 
#' @param job_definition_name name if the AWS Job Definition; default is 'boson-job-definition'
#' @param vcpus number of vcpus to assign for solving job; default value is 1
#' @param memory memory in mb to assign for solving job; default value is 1024
def RegisterBosonJobDefinition(job_definition_name = 'boson-job-definition', vcpus = 1, memory = 1024):
	command = "aws batch register-job-definition --job-definition-name', job_definition_name"
		+ "--type container --container-properties"
		+ "\" {image :" + "757968107665_dkr_ecr_us-west-2_amazonaws_com/boson-docker-image:latest vcpus: {} memory: {}}:".format(vcpus,memory)
    
	os.system(command)


#' Deregister a AWS Batcj Job Definition
#' 
#' @param job_definition_name name if the AWS Job Definition; default is 'boson-job-definition'
def DeregisterBosonJobDefinition (job_definition_name = 'boson-job-definition', revision_id = 1):
	command = "aws batch deregister-job-definition --job-definition {}:{}".format(job_definition_name,revision_id)
	os.system(command)




