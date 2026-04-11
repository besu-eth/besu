#!/usr/bin/env python3
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

import glob
import os
import sys
import xml.etree.ElementTree as ET

count = int(sys.argv[1]) if len(sys.argv) > 1 else 10
runner = sys.argv[2] if len(sys.argv) > 2 else None

results = []
total_tests = 0
for f in glob.glob('**/build/test-results/**/TEST-*.xml', recursive=True):
    try:
        tree = ET.parse(f)
    except ET.ParseError:
        continue
    root = tree.getroot()
    suites = root.findall('testsuite') if root.tag == 'testsuites' else [root]
    for suite in suites:
        name = suite.get('name')
        time = suite.get('time')
        if name and time:
            try:
                results.append((float(time), name))
                total_tests += int(suite.get('tests', 0))
            except ValueError:
                pass

if not results:
    sys.exit(0)

results.sort(reverse=True)
total_classes = len(results)

heading = f'{count} Slowest Test Classes (runner {runner})' if runner else f'{count} Slowest Test Classes'
summary_line = f'{total_tests:,} tests across {total_classes} classes'
lines = [
    f'<details open>\n',
    f'<summary><b>{heading}</b> — {summary_line}</summary>\n\n',
    '| Rank | Class | Time |\n',
    '|------|-------|------|\n',
]
for i, (t, name) in enumerate(results[:count], 1):
    mins = int(t // 60)
    secs = t % 60
    time_str = f'{mins}m {secs:.0f}s' if mins else f'{secs:.1f}s'
    short = name.split('.')[-1]
    lines.append(f'| {i} | `{short}` | {time_str} |\n')
lines.append('\n</details>\n')

summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
if summary_path:
    with open(summary_path, 'a', encoding='utf-8') as f:
        f.writelines(lines)
else:
    sys.stdout.writelines(lines)
