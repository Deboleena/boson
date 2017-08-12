# AWSBatchUtils.R
# run: Rscript AWSBatchUtils.R

require(jsonlite)

SubmitBatchJobs = function (
  batch.id,
  job.type = 'bootstrap-r-jobs',
  njobs,
  job.id = '0',
  task.ids = '0',
  s3.path,
  job.name = 'boson-job',
  job.queue = 'boson-job-queue2',
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
# print(SubmitBatchJobs(batch.id = 0, njobs = 1, s3.path = 's3://boson-base/rboson-test/'))


MonitorJobStatus = function (job.ids) {
  out = system2('aws', c('batch', 'describe-jobs',
                         '--jobs', paste(job.ids, collapse = ' ')),
                stdout = TRUE
                )
  
  df = fromJSON(paste(out, collapse = ''))[[1]][, c('jobId', 'status')]
  
  return(df)
}
# print(MonitorJobStatus(c('3219cdf7-b753-4637-85ef-4a2693e9e8ab', '92c357da-3d62-4b97-bb57-76ad86149316')))