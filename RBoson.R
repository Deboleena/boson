# RBoson.R
# run: Rscript RBoson.R

# load libraries & scripts
source('s3utils.R')
source('AWSBatchUtils.R')


#' Configure AWS-CLI
#' 
#' @param aws.access.key.id aws.access.key.id; required
#' @param aws.secret.access.key aws.secret.access.key; required
#' @param aws.region aws.region; defalut value is 'us-west-2'
#' @param output.format output format; defalute value is 'json'
#' @param profile; default value is 'boson'
#' @export
AWSConfigure = function (
  aws.access.key.id,
  aws.secret.access.key,
  aws.region = 'us-west-2',
  output.format = 'json',
  profile = 'boson'
) {
  # make sure ~/.aws
  dir.create('~/.aws/', showWarnings = FALSE)
  
  # update ~/.aws/credentials
  if (!file.exists('~/.aws/credentials')) {
    file.create('~/.aws/credentials')
  }
  lines = readLines('~/.aws/credentials')
  which.boson = which(lines == paste0('[', profile, ']'))
  if (length(which.boson) > 0) {
    lines = lines[setdiff(1:length(lines), which.boson:(which.boson+2))]
  }
  lines = c(
    lines,
    paste0('[', profile, ']'),
    paste0('aws_access_key_id = ', aws.access.key.id),
    paste0('aws_secret_access_key = ', aws.secret.access.key)
  )
  fileCon = file('~/.aws/credentials')
  writeLines(lines, con = fileCon)
  close(fileCon)
  
  # update ~/.aws/config
  if (!file.exists('~/.aws/config')) {
    file.create('~/.aws/config')
  }
  lines = readLines('~/.aws/config')
  which.boson = which(lines == paste0('[', profile, ']'))
  if (length(which.boson) > 0) {
    lines = lines[setdiff(1:length(lines), which.boson:(which.boson+2))]
  }
  lines = c(
    lines,
    paste0('[', profile, ']'),
    paste0('region = ', aws.region),
    paste0('output = ', output.format)
  )
  fileCon = file('~/.aws/config')
  writeLines(lines, con = fileCon)
  close(fileCon)
  
}
# AWSConfigure(
#   aws.access.key.id = '****',
#   aws.secret.access.key = '****'
# )


#' Setup an environment for executing tasks in parallel using Boson
#'
#' @param comp.env.name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
#' @param instance.types what type of EC2 instance to attach to the Compute Environment; default is 'm4.large'
#' @param min.vcpus minimum number of vcpus to maintain in the Compute Environment; default valus is 0
#' @param max.vcpus maximum number of vcpus to maintain in the Compute Environment; default valus is 2
#' @param initial.vcpus number of vcpus initially attached to the Compute Environment; default valus is 2
#' @param service.role.arn ARN of a role created in AWS IAM with the following policies attached: AmazonS3FullAccess, AWSBatchServiceRole, AWSBatchFullAccess; required
#' @param subnets subnets from AWS VPC; required
#' @param security.group.ids security.group.ids from AWS VPC; required
#' @param job.queue.name name of the AWS Job Queue; default is 'boson-job-queue'
#' @param job.definition.name name if the AWS Job Definition; default is 'boson-job-definition'
#' @export
BosonSetup = function (
	comp.env.name = 'boson-comp-env',
	instance.types = c("m4.large"),
	min.vcpus = 0,
	max.vcpus = 2,
	initial.vcpus = 2,
	service.role.arn,
	subnets,
	security.group.ids,
	job.queue.name = 'boson-job-queue',
	job.definition.name = 'boson-job-definition'
) {
	# create a compute-environment for Boson
  CreateBatchComputeEnvironment (
    comp.env.name = comp.env.name,
    instance.types = instance.types,
    min.vcpus = min.vcpus,
    max.vcpus = max.vcpus,
    initial.vcpus = initial.vcpus,
    service.role.arn = service.role.arn,
    subnets = subnets,
    security.group.ids = security.group.ids
  )

	# create a job queue for Boson
  CreateJobQueue (
    job.queue.name = job.queue.name,
    comp.env.name = comp.env.name
  )

	# register a job-definition for Boson
  RegisterBosonJobDefinition (
    job.definition.name = job.definition.name
  )
}
# BosonSetup (
#   service.role.arn = "arn:aws:iam::757968107665:role/BosonBatch",
#   subnets = c("subnet-1a69d77d","subnet-4da19315","subnet-abfc2ce2"),
#   security.group.ids = "sg-ddd562a7"
# )


#' Cleaup a Boson environment
#' 
#' @param comp.env.name name of the AWS Batch Compute Environment; default is 'boson-comp-env'
#' @param job.queue.name name of the AWS Job Queue; default is 'boson-job-queue'
#' @param job.definition.name name if the AWS Job Definition; default is 'boson-job-definition'
#' @export
BosonCleanup = function (
  comp.env.name = 'boson-comp-env',
  job.queue.name = 'boson-job-queue',
  job.definition.name = 'boson-job-definition'
) {
  
  # deregister a job-definition for Boson
  DeregisterBosonJobDefinition (
    job.definition.name = job.definition.name
  )
  
  # delete the job queue for Boson
  DeleteJobQueue (
    job.queue.name = job.queue.name
  )
  
  # delete the compute-environment for Boson
  DeleteBatchComputeEnvironment (
    comp.env.name = comp.env.name
  )
}
# BosonCleanup()


#'  Submit parallel tasks for batch execution using Boson
#'  
#'  @param X a list of named lists collecting argument tuples of FUN; required
#'  @param FUN a function that solves one parallel task in the batch when run with one argument tuple in X; required
#'  @param ... common arguments shared by all tasks used by FUN; optional
#'  @param njobs number of AWS Batch jobs to spawn for solving all parallel tasks; required
#'  @param s3.bucket an S3 bucket where intermediate files can be stored in sub-folders; required
#'  @param batch.id batch id; default value is NULL, which yields automatic calculation of the batch id
#'  @param s3.path path to an S3 folder; default value is NULL, which causes s3.path to be automatically set by s3.bucket and batch.id
#'  @param bootstarp.agent whetehr to use local R or AWS Batch for bootstrapping jobs; default value is 'localR'
#'  @param job.name name of the Batch job; default value is 'boson-job'
#'  @param job.queue name of the job queue to use; default value is 'boson-job-queue'
#'  @param job.definition name of the job.definition; default value is 'boson-batch-job'
#'  @param region AWS region; default value is 'us-west-2'
#'  @param blocking.call boolean, whether to make SubmitBosonTasks() a blocking call; default value is TRUE
#'  @param ping if blocking.call = TRUE, frequency of printing job status in seconds; default is every 10 seconds
#'  @param print.job.status if blocking.call = TRUE, level of details in printing job status; default value is 'summary'
#'  
#'  @export
SubmitBosonTasks = function (
	X,
	FUN,
	...,
	njobs,
	s3.bucket,
	batch.id = NULL,
	s3.path = NULL,
	bootstarp.agent = c('localR', 'awsBatch'),
	job.name = 'boson-job',
  job.queue = 'boson-job-queue',
  job.definition = 'boson-batch-job',
  region = 'us-west-2',
	blocking.call = TRUE,
  ping = 10,
  print.job.status = c('summary', 'detailed', 'none')
) {

	# create a new batch.id
	if (is.null(batch.id)) {
		objects.in.bucket = S3ListFiles(s3path = s3.bucket)
		batch.folders = gsub('/', '', objects.in.bucket[grepl('batch', objects.in.bucket)])

		if (length(batch.folders) == 0) {
			batch.id = 1
		} else {
			last.id = -1
			for (i in 1:length(batch.folders)) {
				id = as.integer(strsplit(batch.folders[i], '_')[[1]][2])
				if (id > last.id) { last.id = id }
			}
			batch.id = last.id + 1
		}
	}
	cat(paste0("batch.id = ", batch.id, "\n"))

	# create an s3.path (folder in s3.bucket) where inputs and outputs will be stored
	if (is.null(s3.path)) {
		if (substr(s3.bucket, nchar(s3.bucket), nchar(s3.bucket)) == '/') {
			s3.bucket = substr(s3.bucket, 1, nchar(s3.bucket) - 1)
		}
		s3.path = paste0(s3.bucket, '/batch_', batch.id, '/')
	}
	cat(paste0("s3.path = '", s3.path, "'\n"))

	# upload inputs to Batch jobs
	SaveObjectesInS3(
		FUN = BosonTask, X = BosonParams, extra.args = list(...),
	  	s3.path = s3.path,
	  	key = paste0('batch_', batch.id, '_in')
	)

	# Bootstrap jobs
	if (bootstarp.agent[1] == 'localR') {
		df.jobid = BootstrapBatchJobs (
			batch.id = batch.id,
			ntasks = length(X),
			njobs = njobs,
			s3.path = s3.path,
			job.name = job.name,
			job.queue = job.queue,
			job.definition = job.definition,
			region = region
		)
	} else if (bootstarp.agent[1] == 'awsBatch') {
		SubmitBatchJob (
  			batch.id = batch.id,
  			njobs = njobs,
  			s3.path = s3.path, 
  			job.type = "bootstrap-r-jobs", 
  			job.id = '0',
  			task.ids = '0',
  			job.name = job.name, 
  			job.queue = job.queue, 
  			job.definition = job.definition, 
  			region = region
  		)

  		df.jobid = NULL
  		while (is.null(df.jobid)) {
  			Sys.sleep(ping)
  			df.jobid = LoadObjectsFromS3(
  				s3.path = s3.path,
  				key = paste0('batch_', batch.id, '_jobids'),
  				supressWarnings = TRUE
  			)[['df.jobid']]
  		}
	} else {
		stop("Enter a correct boostrap.agent - acceptable values are 'localR', 'awsBatch'")
	}
	cat('Submitted jobs:\n')
	print(df.jobid)

	# wait for jobs to finish if this call is blocking
	if (blocking.call) {
		WaitForJobsToFinish(df.jobid$job.id, ping = ping, print.job.status = print.job.status)
		return ( FetchBatchOutcomes (batch.id, njobs, s3.bucket, s3.path) )
	} else {
		return ( df.jobid )
	}
}
# BosonTask = log
# BosonParams = as.list(1:10)
# out = SubmitBosonTasks (
#   X = BosonParams,
#   FUN = BosonTask,
#   njobs = 2,
#   s3.bucket = 's3://boson-base/',
#   # batch.id = 0,
#   # bootstarp.agent = 'awsBatch',
#   ping = 2,
#   blocking.call = TRUE
#   # print.job.status = 'detailed'
# )
# print(out)


#' Wait until specified jobs are finished
#' 
#' @param job.ids vector of job-ids; required
#' @param ping frequency of printing job status in seconds; default is every 10 seconds
#' @param print.job.status level of details in printing job status; default value is 'summary'
#' @export
WaitForJobsToFinish = function (job.ids, ping = 10, print.job.status = c('summary', 'detailed', 'none')) {
  df.monitor = MonitorJobStatus(job.ids, print.job.status = print.job.status)
  while (!all(df.monitor$status %in% c('SUCCEEDED', 'FAILED'))) {
    Sys.sleep(ping)
    df.monitor = MonitorJobStatus(job.ids, print.job.status = print.job.status)
  }
}


#' Fetch outcomes of jobs submitted as a Boson batch
#' 
#' @param batch.id bacth.id; required
#' @param njobs number of jobs submitted with the batch.id; required
#' @param s3.bucket S3 bucket where intermediate files can be stored in sub-folders; required
#' @param s3.path path to an S3 folder; default value is NULL, which causes s3.path to be automatically set by s3.bucket and batch.id
FetchBatchOutcomes = function (batch.id, njobs, s3.bucket, s3.path = NULL) {
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

  return(out.all)
}
# print(
# 	FetchBatchOutcomes(
# 		batch.id = 3,
# 		njobs = 2,
# 		s3.bucket = 's3://boson-base/'
# 	)
# )

#' Cleanup AWS resources used by a Batch
#' 
#' @param batch.id batch id; required
#' @param s3.bucket S3 bucket where intermediate files can be stored in sub-folders; required
#' @param s3.path path to an S3 folder; default value is NULL, which causes s3.path to be automatically set by s3.bucket and batch.id
BatchCleanup = function (batch.id, s3.bucket, s3.path = NULL) {
	if (is.null(s3.path)) {
		if (substr(s3.bucket, nchar(s3.bucket), nchar(s3.bucket)) == '/') {
			s3.bucket = substr(s3.bucket, 1, nchar(s3.bucket) - 1)
		}
		s3.path = paste0(s3.bucket, '/batch_', batch.id, '/')
	}
	S3DeleteFolder(s3.path)
}

