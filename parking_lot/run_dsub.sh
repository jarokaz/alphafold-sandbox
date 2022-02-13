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

set -o errexit
set -o nounset


readonly DSUB_PROVIDER=google-cls-v2
readonly POLLING_INTERVAL=30s

PROJECT=
ARGS=( "$@" )
for i in "${!ARGS[@]}"
do
    if [[ "${ARGS[i]}" == "--project" ]]; then
        PROJECT="${ARGS[i+1]}"
    fi
     if [[ "${ARGS[i]}" == "--executor_input" ]]; then
        EXECUTOR_INPUT="${ARGS[i+1]}"
    fi
done

echo "$EXECUTOR_INPUT"

exit 0

if [[ -z "$PROJECT" ]]; then
    echo 'No GPC project ID passed'
    exit 2
fi

JOB_ID=$(dsub --provider "$DSUB_PROVIDER" "$@" 2> /dev/null)

echo "Waiting for the job to complete"

JOB_STATUS=" "
PREVIOUS_JOB_STATUS=
while ! [[ -z "$JOB_STATUS" ]]
do
    JOB_STATUS=$(dstat --provider "$DSUB_PROVIDER" --jobs "$JOB_ID" --project "$PROJECT" | head -n 3 | tail -1)
    if [[ $JOB_STATUS != $PREVIOUS_JOB_STATUS ]]
    then
        echo "$JOB_STATUS" 
        PREVIOUS_JOB_STATUS="$JOB_STATUS"
    fi
    sleep "$POLLING_INTERVAL"
    # We should add periodic retrieval of logs to update status in KFP 
done
