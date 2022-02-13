#!/bin/bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a simple wrapper on top of dsub. In this "incarnation"
# we use dsub --wait option to wait for the CLS pipeline to complete
# The next step would be to retrieve status and logs on a more regular
# basis so we can push it back to Vertex Pipelines

SCRIPT_NAME=run_dsub_job

usage()
{
  echo "Usage: run_dsub_job ..."
  exit 2
}

prepare_common_arguments() {
    
}


# Do some light validation of arguments
PARAMS=type:,input-path:,output-path:,database-paths:,disk-image:,machine-type:,\
disk-boot-size:,n-cpu:,max-sto-sequences:,maxseq:

PARSED_ARGUMENTS=$(getopt -a  -n "$SCRIPT_NAME" --options h --longoptions "$DSUBPARAMS" -- "$@")
VALID_ARGUMENTS=$?
if [ "$VALID_ARGUMENTS" != "0" ] 
then
    usage
fi
#TRIMMED_ARGUMENTS=$(sed 's/ -- .*//' <<< "$PARSED_ARGUMENTS")
#eval set -- "$TRIMMED_ARGUMENTS"

#echo "$@"

# Extract non dsub
while :
do 
    case "$1" in 
        --project) PROJECT="$2" ; shift 2 ;;
        -h) usage; break ;;
        --) break ;;
        *) shift 1 ;;
    esac
done

echo $PROJECT




