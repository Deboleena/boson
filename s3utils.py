# S3 Utility functions for PYBOSON
# run: python s3utils.py

import os
import commands
import dill as pickle

# from scipy.stats import norm

#' Create a folder in S3
#' 
#' @param s3path path to an S3 directory

def S3CreateFolder(s3path):
	# create a tmplorary file
	zero_file = open('./0', 'w')

	# make an 'aws s3 cp' call
	command = 'aws s3 cp 0 {}'.format(s3path)
	os.system(command)
	# delete the temporary file locally and on S3
	os.remove("0")

# S3CreateFolder(s3path = 's3://pyboson-base/temp/')

#' List all files in an S3 directory
#' 
#' @param s3path path to an S3 directory
#' @param sanitize boolean, whether to exclude the temporary file '0' from returns; default is TRUE
#' @return a list of files at the s3path as a character vector

def S3ListFiles(s3path, sanitize = True):
	# make an 'aws s3 ls' call and capture
	parse_str = " | awk '{print $4}'"
	command = "aws s3 ls {} {}".format(s3path, parse_str)
	status, output = commands.getstatusoutput(command)

	# parse filenames out of the output
	files = output.split('\n')

	if (sanitize):
		files = [x for x in files if not '/0' in x]
		return files
	else:
		return files

# print(S3ListFiles(s3path = 's3://boson-base/rboson-test/'))

#' Copy files to / from S3
#' 
#' @param source path to source directory (local / S3)
#' @param destination path to destination diectory (local / S3)
#' @param files list of files to be copied as a character vector

def S3CopyFiles(files, source = None, destination = None ):

	if source != None:
		temp = []
		for f in files:
			temp.append(source + f)  
		files = temp

	for f in files:
		command = "aws s3 cp {} {}".format(f, destination)
		os.system(command)


# S3CopyFiles(destination = 's3://boson-base/rboson-test/', files = ['hello_world.R', 'hello_world.yml'])
# S3CopyFiles(
# 	source = 's3://boson-base/rboson-test/',
# 	destination = '/Users/deboleenamukhopadhyay/Google\ Drive/MacDrive/Capstone_project/boson_v1/tmp/',
# 	files = ['hello_world.R', 'hello_world.yml']
# )


#' Delete files from S3
#' 
#' @param s3path path to an S3 directory
#' @param files list of files to be deleetd as a character vector


def S3DeleteFiles(s3path, files):
	
	for f in files:
		fname = s3path + f
		command = "aws s3 rm {}".format(fname)
		os.system(command)

# S3DeleteFiles(s3path = 's3://boson-base/rboson-test/', files = ['hello_world.R', 'hello_world.yml'])

#' Delete a floder in S3 with all of its contents
#' 
#' @param s3path path to an S3 directory


def S3DeleteFolder(s3path):
	# delete all files in s3path
	files = S3ListFiles(s3path, sanitize = False)
	S3DeleteFiles(s3path, files)


# S3DeleteFolder(s3path = 's3://pyboson-base/pyboson-test/')

#' Create a bucket in S3
#' 
#' @param bucket name of the bucket to be created


def S3CreateBucket(bucket):
	# make an 'aws s3 mb' call
	command = "aws s3 mb {}".format(bucket)
	status, output = commands.getstatusoutput(command)

	if status == 0:
		return 'success'
	else:
		return 'error'
  
# print(S3CreateBucket(bucket = 's3://dm-tmp-3186876'))

#' Delete a bucket in S3
#' 
#' @param bucket name of the bucket to be deleted
def S3DeleteBucket(bucket):
	command = "aws s3 rb {}".format(bucket)
	status, output = commands.getstatusoutput(command)

	if status == 0:
		return 'success'
	else:
		return 'error'
# print(S3DeleteBucket(bucket = 's3://dm-tmp-3186876'))



#' Saves python-objects as .p in an S3 folder : pickling

# def SaveObjectesInS3(s3path, key, *args):
# 	pyobjs = args
# 	file_name = key + ".p"
# 	pickle.dump(pyobjs, open( file_name, "wb" ) )
# 	S3CopyFiles(destination = s3path, files = [file_name,])
# 	os.remove(file_name)

# # s3path = "s3://pyboson-base/temp/"
# # r = norm.rvs(size=100)
# # SaveObjectesInS3 (s3path, 'norm', r)

def SaveObjectesInS3(s3_path, key, **kwargs):
	pyobjs = kwargs
	# print pyobjs
	file_name = key + ".p"
	pickle.dump(pyobjs, open( file_name, "wb" ) )
	S3CopyFiles(destination = s3_path, files = [file_name,])
	os.remove(file_name)

# s3path = "s3://pyboson-base/temp/"
# import math
# SaveObjectesInS3 (s3path, 'tmp', FUN = math.log, X = [1,2,3])


# unpickle python object from s3
def LoadObjectsFromS3(s3_path, keys):
	pyobjs = []
	for k in keys:
		file_name = k + ".p"
		S3CopyFiles(source = s3_path, destination = './', files = [file_name,])
		temp_obj = pickle.load(open(file_name, "rb"))
		# print temp_obj
		pyobjs.append(temp_obj)
		os.remove(file_name)

	return pyobjs
# s3path = "s3://pyboson-base/temp/"
# r1 = LoadObjectsFromS3(s3path, ['norm'])
# print(LoadObjectsFromS3(s3path, ['norm']))
# print (type(r1))
