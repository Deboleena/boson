# BosonClient.R
# run: Rscript BosonClient.R
# run: aws batch submit-job --job-name boson-job --job-queue boson-job-queue --job-definition boson-batch-job --container-overrides '{"command":["sh","driver.sh","us-west-2","--bootstrap-jobs","5","s3://boson-base/rboson-base/boson_rjob_0.rdata","boson_rjob_0.rdata","s3://boson-base/tmp/"]}'

# load libraries & scripts
source('s3utils.R')
source('AWSBatchUtils.R')

# params
batch.id = 1
njobs = 4
s3.path = 's3://boson-base/rboson-test/'

#BosonTask = function (x, q) {
# return ( quantile (x, probs = q) )
#}

BosonTask = log
BosonParams = as.list(1:1000)
#for (i in 1:101) { BosonParams[[i]] = list(q = (i-1) / 100) }
#x = rnorm (10000, 0, 1)
#x = log(1:1000)

# upload metadata
SaveObjectesInS3(
  FUN = BosonTask, X = BosonParams, extra.args = list(),
  s3.path = s3.path,
  key = paste0('batch_', batch.id, '_in')
)

# submit a batch job
SubmitBatchJobs(batch.id = batch.id, njobs = njobs, s3.path = s3.path)

# download outcomes
S3ListFiles = function (s3path = s3.path)

out.all = LoadObjectsFromS3(
  s3.path = s3.path,
  keys = paste0('batch_', batch.id, '_out_', 1:njobs)
)
