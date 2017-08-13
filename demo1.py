## EXAMPLE 1 ##

# imports
from s3utils import * 
from AWSBatchUtils import *


# Step 1: Define parallel tasks
def BosonTask(x):
	return( x * x )

BosonParams = []
for x in range(100):
	BosonParams.append( {'x': x} )


# Step 2: Upload inputs and submit a PyBoson batch
batch_id = 1
njobs    = 10
s3_path  = 's3://boson-base/pyboson-test/'

SaveObjectesInS3(s3_path,
	key = 'batch_{}_in'.format(batch_id),
	FUN = BosonTask, X = BosonParams
)

SubmitBatchJobs(batch_id = batch_id, njobs = njobs, s3_path = s3_path)


# Step 3: Collect results
out_all = LoadObjectsFromS3(
	s3_path = s3_path,
	keys = ['batch_{}_out_'.format(batch_id) + s for s in map(str,range(1,njobs+1))]
)
print(out_all)

