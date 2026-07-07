#!/bin/bash
##
## Copyright contributors to Besu.
##
## Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
## the License. You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
## an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
## specific language governing permissions and limitations under the License.
##
## SPDX-License-Identifier: Apache-2.0
##

REPORTS_DIR="$1"
REPORT_STRIP_PREFIX="$2"
REPORT_STRIP_SUFFIX="$3"
SPLIT_COUNT=$4
SPLIT_INDEX=$5

# extract tests time from Junit XML reports
find "$REPORTS_DIR" -type f -name TEST-*.xml | xargs -I{} bash -c "xmlstarlet sel -t -v 'concat(sum(//testcase/@time), \" \", //testsuite/testcase[1]/@classname)' '{}'; echo '{}' | sed \"s#${REPORT_STRIP_PREFIX}/\(.*\)/${REPORT_STRIP_SUFFIX}.*# \1#\"" > tmp/timing.tsv

# Sort times in descending order
IFS=$'\n' sorted=($(sort -nr tmp/timing.tsv))
unset IFS

sums=()
tests=()

# Initialize sums
for ((i=0; i<SPLIT_COUNT; i++))
do
	sums[$i]=0
done

echo -n '' > tmp/processedTests.list

# add tests to groups trying to balance the sum of execution time of each group
for line in "${sorted[@]}"; do
	line_parts=( $line )
	test_time=$( echo "${line_parts[0]} * 1000 / 1" | bc )  # convert to millis without decimals
	test_name=${line_parts[1]}
	module_dir=${line_parts[2]}
	test_with_module="$test_name $module_dir"

  # deduplication check to avoid executing a test multiple time
  if grep -F -q --line-regexp "$test_with_module" tmp/processedTests.list
  then
    continue
  fi

  # Does the test still exists?
  if grep -F -q --line-regexp "$test_with_module" tmp/currentTests.list
  then
    # Find index of min sum
    idx_min_sum=0
    min_sum=${sums[0]}
    for ((i=0; i<SPLIT_COUNT; i++))
    do
      if [[ ${sums[$i]} -lt $min_sum ]]
      then
        idx_min_sum=$i
        min_sum=${sums[$i]}
      fi
    done

    # Add the test to the min sum list
    min_sum_tests=${tests[$idx_min_sum]}
    tests[$idx_min_sum]="${min_sum_tests}${test_with_module},"

    # Update the sums
    ((sums[idx_min_sum]+=test_time))

    echo "$test_with_module" >> tmp/processedTests.list
  fi
done

# Any new test?
grep -F --line-regexp -v -f tmp/processedTests.list tmp/currentTests.list > tmp/newTests.list
idx_new_test=0
while read -r new_test_with_module
do
	idx_group=$(( idx_new_test % SPLIT_COUNT ))
	group=${tests[$idx_group]}
	tests[$idx_group]="${group}${new_test_with_module},"
	idx_new_test=$(( idx_new_test + 1 ))
done < tmp/newTests.list

# remove last comma
for ((i=0; i<SPLIT_COUNT; i++))
do
  test_list=${tests[$i]%,}
  tests[$i]="$test_list"
done


# group tests by module
module_list=( $( echo "${tests[$SPLIT_INDEX]}" | tr "," "\n" | awk '{print $2}' | sort -u ) )

declare -A group_by_module
for module_dir in "${module_list[@]}"
do
	group_by_module[$module_dir]=""
done

IFS="," test_list=( ${tests[$SPLIT_INDEX]} )
unset IFS

for line in "${test_list[@]}"
do
	line_parts=( $line )
	test_name=${line_parts[0]}
	module_dir=${line_parts[1]}

	module_group=${group_by_module[$module_dir]}
	group_by_module[$module_dir]="$module_group$test_name "
done

# return the requests index, without quotes to drop the last trailing space
for module_dir in "${module_list[@]}"
do
	module_test_task=":${module_dir//\//:}:test"
	module_tests=$( echo "${group_by_module[$module_dir]% }" | sed -e 's/^\| / --tests /g' )
	echo "$module_test_task $module_tests"
done
