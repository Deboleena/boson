# AWSBatchUtils.R
# run: Rscript AWSBatchUtils.R

require(jsonlite)

#' Submit a job to either bootstrap more jobs or solve tasks
#' 
#'  @param batch.id batch id; required
#'  @param job.type job type, can be 'bootstrap-r-jobs' or 'run-r-tasks'; default value is 'bootstrap-r-jobs'
#'  @param njobs if job.type = 'bootstrap-r-jobs', number of AWS Batch jobs to spawn for solving all parallel tasks; required
#'  @param s3.path path to an S3 folder; required
#'  @param job.id job.type = 'run-r-tasks', the job id; default value is '0'
#'  @param task.ids job.type = 'run-r-tasks', the task ids to solve as one job; default value is '0'
#'  @param job.name name of the Batch job; default value is 'boson-job'
#'  @param job.queue name of the job queue to use; default value is 'boson-job-queue'
#'  @param job.definition name of the job.definition; default value is 'boson-batch-job'
#'  @param region AWS region; default value is 'us-west-2'
SubmitBatchJob = function (
  batch.id,
  job.type = c('bootstrap-r-jobs', 'run-r-tasks'),
  njobs,
  s3.path,
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
                     '","', job.type[1],
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

#' Bootstrap jobs
#' 
#'  @param batch.id batch id; required
#'  @param ntasks number of tasks to solve; required
#'  @param njobs number of AWS Batch jobs to spawn for solving all parallel tasks; required
#'  @param s3.path path to an S3 folder; required
#'  @param job.name name of the Batch job; default value is 'boson-job'
#'  @param job.queue name of the job queue to use; default value is 'boson-job-queue'
#'  @param job.definition name of the job.definition; default value is 'boson-batch-job'
#'  @param region AWS region; default value is 'us-west-2'
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


#' Monitor status of AWS Batch jobs
#' 
#' @param job.ids vector of job-ids; required
#' @param ping frequency of printing job status in seconds; default is every 10 seconds
#' @param print.job.status level of details in printing job status; default value is 'summary'
MonitorJobStatus = function (job.ids, print.job.status = c('summary', 'detailed', 'none')) {
  out = system2('aws', c('batch', 'describe-jobs',
                         '--jobs', paste(job.ids, collapse = ' ')),
                stdout = TRUE
                )
  
  df = jsonlite::fromJSON(paste(out, collapse = ''))[[1]][, c('jobId', 'status')]
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

#' Create a AWS Batch Compute Environment
#' 
#' @param comp.env.name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
#' @param instance.types what type of EC2 instance to attach to the Compute Environment; default is 'm4.large'
#' @param min.vcpus minimum number of vcpus to maintain in the Compute Environment; default valus is 0
#' @param max.vcpus maximum number of vcpus to maintain in the Compute Environment; default valus is 2
#' @param initial.vcpus number of vcpus initially attached to the Compute Environment; default valus is 2
#' @param service.role.arn ARN of a role created in AWS IAM with the following policies attached: AmazonS3FullAccess, AWSBatchServiceRole, AWSBatchFullAccess; required
#' @param subnets subnets from AWS VPC; required
#' @param security.group.ids security.group.ids from AWS VPC; required
CreateBatchComputeEnvironment = function (
  comp.env.name = 'boson-comp-env',
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

  # wait till up
  flag = TRUE
  while (flag) {
    out = system2('aws', c(
        'batch', 'describe-compute-environments',
        '--compute-environments', comp.env.name
      ),
      stdout = TRUE
    )
    # print(paste(out, collapse = ''))
    flag = ! (grepl(comp.env.name, paste(out, collapse = '')) && grepl('ENABLED', paste(out, collapse = '')))
    Sys.sleep(1)
  }
}
# CreateBatchComputeEnvironment (
#   service.role.arn = "arn:aws:iam::757968107665:role/BosonBatch",
#   subnets = c("subnet-1a69d77d","subnet-4da19315","subnet-abfc2ce2"),
#   security.group.ids = "sg-ddd562a7"
# )

#' Delete a AWS Batch Compute Environment
#' 
#' @param comp.env.name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
DeleteBatchComputeEnvironment = function (
  comp.env.name = 'boson-comp-env'
) {
  # disable
  out = system2('aws', c(
    'batch', 'update-compute-environment',
    '--compute-environment', comp.env.name,
    '--state', 'DISABLED'
    ),
    stdout = T
  )

  # wait till disabled
  flag = TRUE
  while (flag) {
    out = system2('aws', c(
        'batch', 'describe-compute-environments',
        '--compute-environments', comp.env.name
      ),
      stdout = TRUE
    )
    # print(paste(out, collapse = ''))
    flag = grepl('ENABLED', paste(out, collapse = ''))
    Sys.sleep(1)
  }

  # delete
  system2('aws', c(
    'batch', 'delete-compute-environment',
    '--compute-environment', comp.env.name
    )
  )
}
# DeleteBatchComputeEnvironment()

#' Create a AWS Batch Job Queue
#' 
#' @param job.queue.name name of the AWS Job Queue; default is 'boson-job-queue'
#' @param comp.env.name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
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

  # wait till up
  flag = TRUE
  while (flag) {
    out = system2('aws', c(
        'batch', 'describe-job-queues',
        '--job-queues', job.queue.name
      ),
      stdout = TRUE
    )
    # print(paste(out, collapse = ''))
    flag = ! (grepl(job.queue.name, paste(out, collapse = '')) && grepl('ENABLED', paste(out, collapse = '')))
    Sys.sleep(1)
  }
}
# CreateJobQueue(job.queue.name = 'boson-queue-2', comp.env.name = 'boson-5')

#' Delete a AWS Batch Job Queue
#' 
#' @param job.queue.name name of the AWS Job Queue; default is 'boson-job-queue'
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

  # wait till disabled
  flag = TRUE
  while (flag) {
    out = system2('aws', c(
        'batch', 'describe-job-queues',
        '--job-queues', job.queue.name
      ),
      stdout = TRUE
    )
    # print(paste(out, collapse = ''))
    flag = grepl('ENABLED', paste(out, collapse = ''))
    Sys.sleep(1)
  }

  # delete
  system2('aws', c(
    'batch', 'delete-job-queue',
    '--job-queue', job.queue.name
    )
  )
}
# DeleteJobQueue(job.queue.name = 'boson-queue-2')

#' Register a AWS Batcj Job Definition
#' 
#' @param job.definition.name name if the AWS Job Definition; default is 'boson-job-definition'
#' @param vcpus number of vcpus to assign for solving job; default value is 1
#' @param memory memory in mb to assign for solving job; default value is 1024
RegisterBosonJobDefinition = function (
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
# RegisterBosonJobDefinition()

#' Deregister a AWS Batcj Job Definition
#' 
#' @param job.definition.name name if the AWS Job Definition; default is 'boson-job-definition'
DeregisterBosonJobDefinition = function (
  job.definition.name = 'boson-job-definition',
  revision.id = 1
) {
  system2('aws', c(
    'batch', 'deregister-job-definition',
    '--job-definition', paste0(job.definition.name, ':', revision.id)
    )
  )
}
# DeregisterBosonJobDefinition(revision.id = 3)

