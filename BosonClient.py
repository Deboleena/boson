# BosonClient.py
# run: python BosonClient.py
# run: aws batch submit-job --job-name boson-job --job-queue boson-job-queue --job-definition boson-batch-job --container-overrides '{"command":["sh","driver.sh","us-west-2","--bootstrap-jobs","5","s3://boson-base/rboson-base/boson_rjob_0.rdata","boson_rjob_0.rdata","s3://boson-base/tmp/"]}'

# load libraries & scripts
from s3utils import * 
from AWSBatchUtils import *

# params
batch_id = 1
njobs = 4
s3_path = 's3://boson-base/pyboson-test/'

def BosonTask(w, x, y, z):
	import math
	return(w * math.log(x) + y - z)
# BosonTask(x = 1, y = 2, z = 3)

BosonParams = []; y = 10; z = 10; w = 1
for i in range(100):
	BosonParams.append({'x':i, 'y':y})

# upload metadata
SaveObjectesInS3(s3_path,
	key = 'batch_{}_in'.format(batch_id),
	FUN = BosonTask, X = BosonParams, w = w, z = z)
import sys
sys.exit(0)

# submit a batch job
SubmitBatchJobs(batch_id = batch_id, njobs = njobs, s3_path = s3_path)

# download outcomes
S3ListFiles(s3_path)

out_all = LoadObjectsFromS3( s3_path = s3_path, keys = ['batch_{}_out_'.format(batch_id) + s for s in map(str,range(1,njobs))] )


