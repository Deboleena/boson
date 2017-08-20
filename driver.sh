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

# call BosonJobMaster with appropriate parameters
if [ "$JOBTYPE" == "bootstrap-r-jobs" ] || [ "$JOBTYPE" == "run-r-tasks" ]; then

	Rscript BosonJobMaster.R $BATCHID $JOBTYPE $NJOBS $JOBID $TASKIDS $S3_PATH

elif [ "$JOBTYPE" == "bootstrap-py-jobs" ] || [ "$JOBTYPE" == "run-py-tasks" ]; then
	
	python BosonJobMaster.py $BATCHID $JOBTYPE $NJOBS $JOBID $TASKIDS $S3_PATH

fi

