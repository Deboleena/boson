# run: python AWSBatchUtils.py

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
				" --container-overrides '{\"command\":[\"sh\",\"driver.sh\"," + \
				"\"" + region + "\"," + \
				"\"" + str(batch_id) + "\"," + \
				"\"" + job_type + "\"," + \
				"\"" + str(njobs) + "\"," + \
				"\"" + str(job_id) + "\"," + \
				"\"" + task_ids + "\"," + \
				"\"" + s3_path + "\"]}'"
	# print command
	status, out = commands.getstatusoutput(command)

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

