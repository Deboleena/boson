# BosonJobMaster.R
# run: Rscript BosonJobMaster.R 0 bootstrap-r-jobs 2 0 0 s3://boson-base/rboson-test/

# run: Rscript BosonJobMaster.R 0 run-r-tasks 1 1 1 s3://boson-base/rboson-test/
# run: Rscript BosonJobMaster.R 0 run-r-tasks 1 1 1,2,3,4,5 s3://boson-base/rboson-test/

# load libraries & scripts
source('s3utils.R')
source('AWSBatchUtils.R')

# parse arguments
args = commandArgs(TRUE)
print('Reading job arguments:'); print(args)
if (length(args) == 6) {
  batch.id  = as.character(args[1])
  job.type  = as.character(args[2])
  njobs     = as.integer(args[3])
  job.id    = as.integer(args[4])
  task.ids  = as.integer(strsplit(args[5], ',')[[1]])
  s3.path   = as.character(args[6])
} else {
  stop("Need to pass 6 arguments")
}

# get metadata and load in environment
print('Loading inputs:')
meta = LoadObjectsFromS3(
  s3.path = s3.path,
  keys = c(paste0('batch_', batch.id, '_in'))
)
list2env(meta, envir = environment())

if (job.type == 'bootstrap-r-jobs') {
  
  # bootstrap jobs in the batch
  df.jobid = BootstrapBatchJobs (
    batch.id = batch.id,
    ntasks = length(X),
    njobs = njobs,
    s3.path = s3.path
  )
  print(df.jobid)
  
  # save outcome in S3
  SaveObjectesInS3(
    out = list(df.jobid = df.jobid),
    s3.path = s3.path,
    key = c(paste0('batch_', batch.id, '_jobids'))
  )
  
} else if (job.type == 'run-r-tasks') {
  
  # run assigned jobs
  out = list()
  if (!exists('extra.args')) {
    extra.args = list()
  }
  
  for (i in task.ids) {
    cat(paste('** Starting job', i, '**\n'))
    tryCatch({
      out[[length(out)+1]] = do.call(FUN, append(X[[i]], extra.args))
      names(out)[length(out)] = names(X)[i]
    }, error = function(e) {
      cat(paste('Error on task', i, '\n'))
    },
    finally = cat(paste('Completed task', i, '\n\n'))
    )
  }
  
  # save outcome in S3
  SaveObjectesInS3(
    out = out,
    s3.path = s3.path,
    key = c(paste0('batch_', batch.id, '_out_', job.id))
  )
  
}



