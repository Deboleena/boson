# Miscellaneous utility functions for RBOSON

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


