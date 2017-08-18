# RBoson.R
# run: Rscript RBoson.R

# load libraries & scripts
source('s3utils.R')
source('AWSBatchUtils.R')


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
	job.definition.name = 'boson-job-definition',
	vcpus = 1,
  	memory = 1024
) {
	# create a compute-environment for Boson

	# create a job queue for Boson

	# register a job-definition for Boson

}


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
    ping = 10,
    blocking.call = FALSE,
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



WaitForJobsToFinish = function (job.ids, ping = 10, print.job.status = c('summary', 'detailed', 'none')) {
  df.monitor = MonitorJobStatus(job.ids, print.job.status = print.job.status)
  while (!all(df.monitor$status %in% c('SUCCEEDED', 'FAILED'))) {
    Sys.sleep(ping)
    df.monitor = MonitorJobStatus(job.ids, print.job.status = print.job.status)
  }
}



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


BatchCleanup = function (batch.id, s3.bucket, s3.path = NULL) {
	if (is.null(s3.path)) {
		if (substr(s3.bucket, nchar(s3.bucket), nchar(s3.bucket)) == '/') {
			s3.bucket = substr(s3.bucket, 1, nchar(s3.bucket) - 1)
		}
		s3.path = paste0(s3.bucket, '/batch_', batch.id, '/')
	}
	S3DeleteFolder(s3.path)
}


BosonClenaup = function (
) {
	# deregister Boson job definitions

	# deactivate and delete Boson job queue

	# deactivate and delete Boson compute environment

	# delete AWS credentials

}



# run
BosonTask = log
BosonParams = as.list(1:10)
out = SubmitBosonTasks(
	X = BosonParams,
	FUN = BosonTask,
	njobs = 2,
	s3.bucket = 's3://boson-base/',
	# batch.id = 0,
	# bootstarp.agent = 'awsBatch',
	ping = 2,
	blocking.call = TRUE
	# print.job.status = 'detailed'
)
print(out)