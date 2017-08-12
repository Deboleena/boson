# driver.sh
# run: sh driver.sh us-west-2 1 run-py-tasks 4 1 1,2,3 s3://boson-base/pyboson-test/

# argument 1: region
# argument 2: bactid
# argument 3: bootstrap-r-jobs or run-r-job-in-queue
# argument 4: number of jobs to be bootstrapped
# argument 5; job id
# argument 6: task ids
# argument 7: s3://path/

# parse arguments
export AWS_DEFAULT_REGION=$1
BATCHID=$2
JOBTYPE=$3
NJOBS=$4
JOBID=$5
TASKIDS=$6
S3_PATH=$7

# if [ "$JOBTYPE" == "bootstrap-r-jobs" ]; then
# 	echo "bootstrapping $NJOBS jobs"

# 	for i in $(seq 1 $NJOBS)
# 	do
# 		echo "submitting job $i in the job-queue"
# 		aws batch submit-job --job-name boson-job --job-queue boson-job-queue --job-definition boson-batch-job --container-overrides "{\"command\":[\"sh\",\"driver.sh\",\"$AWS_DEFAULT_REGION\",\"$BATCHID\",\"run-r-job-in-queue\",\"$NJOBS\",\"$i\",\"$S3_PATH\"]}"
# 	done
# elif [ "$JOBTYPE" == "run-r-job-in-queue" ]; then
# 	echo "running job $JOBID"

# 	# run jobs
# 	Rscript BosonJobMaster.R $BATCHID $JOBID $S3_PATH
# fi

if [ "$JOBTYPE" == "bootstrap-r-jobs" ] || [ "$JOBTYPE" == "run-r-tasks" ]; then

	Rscript BosonJobMaster.R $BATCHID $JOBTYPE $NJOBS $JOBID $TASKIDS $S3_PATH

elif [ "$JOBTYPE" == "bootstrap-py-jobs" ] || [ "$JOBTYPE" == "run-py-tasks" ]; then
	
	python BosonJobMaster.py $BATCHID $JOBTYPE $NJOBS $JOBID $TASKIDS $S3_PATH

fi

