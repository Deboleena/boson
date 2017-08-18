# AWSBatchUtils.R
# run: Rscript AWSBatchUtils.R

require(jsonlite)

SubmitBatchJob = function (
  batch.id,
  njobs,
  s3.path,
  job.type = 'bootstrap-r-jobs',
  job.id = 0,
  task.ids = '0',
  job.name = 'boson-job',
  job.queue = 'boson-job-queue',
  job.definition = 'boson-batch-job',
  region = 'us-west-2'
) {
  
  out = system2('aws', c('batch', 'submit-job',
                   '--job-name', job.name,
                   '--job-queue', job.queue,
                   '--job-definition', job.definition,
                   '--container-overrides',
                   paste0(
                     '\'{"command":["sh","driver.sh","',
                     region,
                     '","', batch.id,
                     '","', job.type,
                     '","', njobs,
                     '","', job.id,
                     '","', task.ids,
                     '","', s3.path,
                     '"]}\'')
                   ),
          stdout = TRUE
  )
  
  job.id = fromJSON(paste(out, collapse = ''))$jobId
  
  return(job.id)
}
# run 5 tasks
# print(SubmitBatchJobs(
#   batch.id = 0,
#   njobs = 5,
#   s3.path = "s3://boson-base/rboson-test/", 
#   job.type = "run-r-tasks", 
#   job.id = '0',
#   task.ids = '1,2,3,4,5',
#   job.name = 'boson-job', 
#   job.queue = 'boson-job-queue', 
#   job.definition = 'boson-batch-job', 
#   region = 'us-west-2'))

# bootstrap jobs
# print(SubmitBatchJob(
#   batch.id = 0,
#   njobs = 4,
#   s3.path = "s3://boson-base/rboson-test/", 
#   job.type = "bootstrap-r-jobs", 
#   job.id = '0',
#   task.ids = '0',
#   job.name = 'boson-job', 
#   job.queue = 'boson-job-queue', 
#   job.definition = 'boson-batch-job', 
#   region = 'us-west-2'))


BootstrapBatchJobs = function (
  batch.id,
  ntasks,
  njobs,
  s3.path,
  job.name = 'boson-job',
  job.queue = 'boson-job-queue',
  job.definition = 'boson-batch-job',
  region = 'us-west-2'
) {

  # partition tasks
  if (ntasks == 1) {
    task.partitions = list(`1` = c(1))
  } else {
    task.partitions = split(1:ntasks, cut(seq_along(1:ntasks), njobs, labels = FALSE))
  }
  
  # submit jobs
  df = data.frame(job.idx = integer(njobs), task.ids = character(njobs), job.id = character(njobs), stringsAsFactors = FALSE)
  job.idx = 0
  for (t in task.partitions) {
    job.idx = job.idx + 1
    task.ids = paste(t, collapse = ',')
    print(paste('Submitting tasks', task.ids, 'in job', job.idx))
    df$job.idx[job.idx] = job.idx
    df$task.ids[job.idx] = task.ids
    df$job.id[job.idx] = SubmitBatchJob (
      batch.id = batch.id,
      job.type = 'run-r-tasks',
      njobs = 1,
      job.id = as.character(job.idx),
      task.ids = task.ids,
      s3.path = s3.path,
      job.name = job.name, 
      job.queue = job.queue, 
      job.definition = job.definition,  
      region = region
    )
  }

  return(df)
}
# print(
#   BootstrapBatchJobs (
#     batch.id = 0,
#     ntasks = 5,
#     njobs = 2,
#     s3.path = "s3://boson-base/rboson-test/"
#   )
# )

MonitorJobStatus = function (job.ids, print.job.status = c('summary', 'detailed', 'none')) {
  out = system2('aws', c('batch', 'describe-jobs',
                         '--jobs', paste(job.ids, collapse = ' ')),
                stdout = TRUE
                )
  
  df = fromJSON(paste(out, collapse = ''))[[1]][, c('jobId', 'status')]
  if (print.job.status[1] == 'summary') {
    cat(paste0(format(Sys.time()), ' - '))
    tab = table(df$status)
    cat(paste(names(tab), tab, sep = ':')); cat('\n')
  } else if (print.job.status[1] == 'detailed') {
    print(format(Sys.time()))
    print(df)
  }
  
  return(df)
}
# print(MonitorJobStatus(c('ee10c476-2bfe-4ae6-bdad-f54146ba84fc', 'a45c7e5f-91c1-487c-9a6d-0d1d6ab313c6'), print.job.status = 'none'))


WaitForJobsToFinish = function (job.ids, ping = 10, print.job.status = c('summary', 'detailed', 'none')) {
  df.monitor = MonitorJobStatus(job.ids, print.job.status = print.job.status)
  while (!all(df.monitor$status %in% c('SUCCEEDED', 'FAILED'))) {
    Sys.sleep(ping)
    df.monitor = MonitorJobStatus(job.ids, print.job.status = print.job.status)
  }
}


FetchBatchOutcomes = function (batch.id, s3.bucket, s3.path = NULL) {
  if (is.null(s3.path)) {
    if (substr(s3.bucket, nchar(s3.bucket), nchar(s3.bucket)) == '/') {
      s3.bucket = substr(s3.bucket, 1, nchar(s3.bucket) - 1)
    }
    s3.path = paste0(s3.bucket, '/batch_', batch.id, '/')
  }

  out.all = LoadObjectsFromS3(
    s3.path = s3.path, 
    keys = paste0('batch_', batch.id, '_out_', 1:njobs)
  )

  retunr(out.all)
}