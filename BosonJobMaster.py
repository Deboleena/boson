# run: python BosonJobMaster.py 1 bootstrap-py-jobs 4 0 0 s3://boson-base/pyboson-test/
# run: python BosonJobMaster.py 1 run-py-tasks 4 1 1,2,3,4 s3://boson-base/pyboson-test/

import sys
import numpy as np

from s3utils import * 
from AWSBatchUtils import *

# parse arguments
arguments = sys.argv[1:]
print 'Reading job arguments:\n {}'.format(arguments)

if (len(arguments) == 6):
	batch_id  = arguments[0]
	job_type  = arguments[1]
	njobs     = int(arguments[2])
	job_id    = int(arguments[3])
	task_ids  = map(int, arguments[4].split(","))
	s3_path   = arguments[5]
	print batch_id, job_type, njobs, job_id, task_ids, s3_path 
else :
	print "Need to pass 6 arguments"
	sys.exit()

# get metadata and load in environment
print('Loading inputs:')
meta = LoadObjectsFromS3(s3_path = s3_path, 
	keys = ['batch_{}_in'.format(batch_id),])[0]
FUN = meta['FUN']
X   = meta['X']
extra_args = {key: meta[key] for key in meta.keys() if key not in ['FUN', 'X']}

if (job_type == 'bootstrap-py-jobs') :
	## bootstrap py-jobs ##
	num_tasks = len(X)
	if (num_tasks == 1) :
		task_partitions = [[1,],]
	else :
		tasks = np.arange(num_tasks)
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

	SaveObjectesInS3(
		s3_path = s3_path,
		key = 'batch_{}_jobids'.format(batch_id),
		out = out)

elif (job_type == 'run-py-tasks'):
	# run py-jobs 
	# run assigned jobs
	out = {}

	for i in task_ids :
		print '** Starting job{} **\n'.format(i)
		try:
			all_args = dict(X[i], **extra_args)
			out[str(i)] = FUN(**all_args)
		except:
			print('error in task' + str(i))

  	# save outcome in S3
  	SaveObjectesInS3(
  		s3_path = s3_path,
  		key = 'batch_{}_out_{}'.format(batch_id,job_id),
    	out = out)