/*
 * Copyright ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

class LogFilter extends Filter {

  private final BlockParameter fromBlock;
  private final BlockParameter toBlock;
  private final LogsQuery logsQuery;

  private final List<LogWithMetadata> logs = new ArrayList<>();

  LogFilter(
      final String id,
      final BlockParameter fromBlock,
      final BlockParameter toBlock,
      final LogsQuery logsQuery,
      final Duration expireDuration) {
    super(id, expireDuration);
    this.fromBlock = fromBlock;
    this.toBlock = toBlock;
    this.logsQuery = logsQuery;
  }

  public BlockParameter getFromBlock() {
    return fromBlock;
  }

  public BlockParameter getToBlock() {
    return toBlock;
  }

  public LogsQuery getLogsQuery() {
    return logsQuery;
  }

  void addLogs(final List<LogWithMetadata> logs) {
    this.logs.addAll(logs);
  }

  List<LogWithMetadata> logs() {
    return logs;
  }

  void clearLogs() {
    logs.clear();
  }
}
