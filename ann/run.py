# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
# I imported the following packages
import os, boto3, re
import calendar
from datetime import datetime


"""A rudimentary timer for coarse-grained profiling"""

class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
  # Call the AnnTools pipeline
  if len(sys.argv) > 1:
    with Timer():
      driver.run(sys.argv[1], 'vcf')
    job_id = sys.argv[2]
    table = boto3.resource('dynamodb', region_name='us-east-1').Table('pramonetti_annotations')
    item = table.get_item(Key={'job_id': job_id})["Item"]
    user_id = item["user_id"]
    addr = sys.argv[1].split("/")
    maindir = addr[0]+"/"+addr[1]+"/"+addr[2]
    files = os.listdir(maindir)
    s3_client = boto3.client('s3')
    results_key = None
    log_key = None
    # Using a similar approach as I did in A4
    for file in files:
      check_log = re.search('.vcf.count.log$', file)
      check = re.search('.annot.vcf$', file)
      if check_log or check:
        object_name = f"pramonetti/{user_id}/{job_id}/{file}"
        if check:
          results_key = object_name
        else:
          log_key = object_name
        file_path = addr[0]+"/"+ addr[1] +"/"+ addr[2] + "/" + file
        #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        s3_client.upload_file(file_path, "gas-results",object_name)
    os.system(f"rm -r jobs/userx/{job_id}")
    try:
      table.update_item(Key={'job_id': job_id},
      UpdateExpression="set #job_status=:j, #s3_results_bucket=:b, #s3_key_result_file=:r, #s3_key_log_file=:l, #complete_time=:c",
      ExpressionAttributeNames={
      '#job_status': 'job_status',
      '#s3_results_bucket': 's3_results_bucket',
      '#s3_key_result_file': 's3_key_result_file',
      '#s3_key_log_file': 's3_key_log_file',
      '#complete_time': 'complete_time'},
      ExpressionAttributeValues={
      ':j': 'COMPLETED',
      ':b': "gas-results",
      ':r': results_key,
      ':l': log_key,
      ':c': calendar.timegm(datetime.now().timetuple())})
    except ClientError as err:
      response = make_response(jsonify(code = 500,status = "error", message = "Could not update database"))
      raise response
  else:
    print("A valid .vcf file must be provided as input to this program.")
