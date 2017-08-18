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


CreateBatchComputeEnvironment = function (
  comp.env.name = 'boson-batch',
  instance.types = c("m4.large"),
  min.vcpus = 0,
  max.vcpus = 2,
  initial.vcpus = 2,
  service.role.arn,
  subnets,
  security.group.ids
) {
  system2('aws', c(
    'batch', 'create-compute-environment',
    '--compute-environment-name', comp.env.name,
    '--type', 'MANAGED',
    '--state', 'ENABLED',
    '--compute-resources', paste0(
      'type="EC2"',
      ',minvCpus=', min.vcpus,
      ',maxvCpus=', max.vcpus,
      ',desiredvCpus=', initial.vcpus,
      ',instanceTypes=', paste(instance.types,collapse = ','),
      ',subnets=', paste0(subnets, collapse = ','),
      ',securityGroupIds=', paste0(security.group.ids, collapse = ','),
      ',instanceRole="ecsInstanceRole"'
      ),
    '--service-role', service.role.arn
    )
  )
}
# CreateBatchComputeEnvironment (
#   service.role.arn = "arn:aws:iam::757968107665:role/BosonBatch",
#   subnets = c("subnet-1a69d77d","subnet-4da19315","subnet-abfc2ce2"),
#   security.group.ids = "sg-ddd562a7"
# )


DeleteBatchComputeEnvironment = function (
  comp.env.name = 'boson-comp-env'
) {
  # disable
  system2('aws', c(
    'batch', 'update-compute-environment',
    '--compute-environment', comp.env.name,
    '--state', 'DISABLED'
    )
  )

  # wait 10 seconds
  Sys.sleep(10)

  # delete
  system2('aws', c(
    'batch', 'delete-compute-environment',
    '--compute-environment', comp.env.name
    )
  )
}
# DeleteBatchComputeEnvironment()


CreateJobQueue = function (
  job.queue.name = 'boson-job-queue',
  comp.env.name = 'boson-comp-env'
) {
  system2('aws', c(
    'batch', 'create-job-queue',
    '--job-queue-name', job.queue.name,
    '--state', 'ENABLED',
    '--priority', '1',
    '--compute-environment-order', paste0('order=1,computeEnvironment=',comp.env.name)
    )
  )
}
# CreateJobQueue()


DeleteJobQueue = function (
  job.queue.name = 'boson-job-queue'
) {
  # disable
  system2('aws', c(
    'batch', 'update-job-queue',
    '--job-queue', job.queue.name,
    '--state', 'DISABLED'
    )
  )

  # wait 10 seconds
  Sys.sleep(10)

  # delete
  system2('aws', c(
    'batch', 'delete-job-queue',
    '--job-queue', job.queue.name
    )
  )
}
# DeleteJobQueue()


RegisterBosonbJobDefinition = function (
  job.definition.name = 'boson-job-definition',
  vcpus = 1,
  memory = 1024
) {
  system2('aws', c(
    'batch', 'register-job-definition',
    '--job-definition-name', job.definition.name,
    '--type','container',
    '--container-properties', paste0(
        '\'{"image": "757968107665.dkr.ecr.us-west-2.amazonaws.com/boson-docker-image:latest", "vcpus": ', vcpus,', "memory": ', memory,'}\''
      )
    )
  )
}
# RegisterBosonbJobDefinition()


DeregisterBosonbJobDefinition = function (
  job.definition.name = 'boson-job-definition',
  revision.id = 1
) {
  system2('aws', c(
    'batch', 'deregister-job-definition',
    '--job-definition', paste0(job.definition.name, ':', revision.id)
    )
  )
}
# DeregisterBosonbJobDefinition(revision.id = 1)

# DeleteBatchLogs = function () {
#   system2('aws', c('logs', 'delete-log-group', '--log-group-name', '/aws/batch/job'))
# }
# DeleteBatchLogs()