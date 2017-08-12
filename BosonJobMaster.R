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
  ## bootstrap r-jobs ##
  
  num.tasks = length(X)
  if (num.tasks == 1) {
    task.partitions = list(`1` = c(1))
  } else {
    task.partitions = split(1:num.tasks, cut(seq_along(1:num.tasks), njobs, labels = FALSE))
  }
  
  out = data.frame(task.ids = character(0), job.id = character(0))
  job.idx = 0
  for (t in task.partitions) {
    job.idx = job.idx + 1
    task.ids = paste(t, collapse = ',')
    print(paste('Submitting tasks:', task.ids))
    out = rbind(out,
      data.frame(
        task.ids = task.ids,
        job.id = SubmitBatchJobs(
          batch.id = batch.id,
          job.type = 'run-r-tasks',
          njobs = 1,
          job.id = as.character(job.idx),
          task.ids = task.ids,
          s3.path = s3.path
        )
      )
    )
  }
  
  # save outcome in S3
  SaveObjectesInS3(
    out = out,
    s3.path = s3.path,
    key = c(paste0('batch_', batch.id, '_jobids'))
  )
  
} else if (job.type == 'run-r-tasks') {
  ## run r-jobs ##
  
  # run assigned jobs
  out = list()
  
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



