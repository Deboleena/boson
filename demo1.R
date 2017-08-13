## EXAMPLE 1 ##

# imports
source('s3utils.R')
source('AWSBatchUtils.R')


# Step 1: Define tasks
BosonTask = function (x) {
	return ( x * x )
}

BosonParams = as.list(1:100)


# Step 2: Upload inputs and submit a RBoson batch
batch.id = 0
njobs    = 10
s3.path  = 's3://boson-base/rboson-test/'

SaveObjectesInS3(
  FUN = BosonTask, X = BosonParams,
  s3.path = s3.path,
  key = paste0('batch_', batch.id, '_in')
)

SubmitBatchJobs(batch.id = batch.id, njobs = njobs, s3.path = s3.path)


# Step 3: Collect results
out.all = LoadObjectsFromS3(
  s3.path = s3.path,
  keys = paste0('batch_', batch.id, '_out_', 1:njobs)
)