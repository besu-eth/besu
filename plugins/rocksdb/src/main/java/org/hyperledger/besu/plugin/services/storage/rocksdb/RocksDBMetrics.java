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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

/** The Rocks db metrics. */
public class RocksDBMetrics {

  private final OperationTimer readLatency;
  private final OperationTimer multiReadLatency;
  private final OperationTimer removeLatency;
  private final OperationTimer writeLatency;
  private final OperationTimer commitLatency;
  private final Counter rollbackCount;

  /**
   * Instantiates a new RocksDb metrics.
   *
   * @param readLatency the read latency
   * @param multiReadLatency the multi read latency
   * @param removeLatency the remove latency
   * @param writeLatency the write latency
   * @param commitLatency the commit latency
   * @param rollbackCount the rollback count
   */
  public RocksDBMetrics(
      final OperationTimer readLatency,
      final OperationTimer multiReadLatency,
      final OperationTimer removeLatency,
      final OperationTimer writeLatency,
      final OperationTimer commitLatency,
      final Counter rollbackCount) {
    this.readLatency = readLatency;
    this.multiReadLatency = multiReadLatency;
    this.removeLatency = removeLatency;
    this.writeLatency = writeLatency;
    this.commitLatency = commitLatency;
    this.rollbackCount = rollbackCount;
  }

  /**
   * Gets read latency.
   *
   * @return the read latency
   */
  public OperationTimer getReadLatency() {
    return readLatency;
  }

  /**
   * Gets multiread latency.
   *
   * @return the multiread latency
   */
  public OperationTimer getMultiReadLatency() {
    return multiReadLatency;
  }

  /**
   * Gets remove latency.
   *
   * @return the remove latency
   */
  public OperationTimer getRemoveLatency() {
    return removeLatency;
  }

  /**
   * Gets write latency.
   *
   * @return the write latency
   */
  public OperationTimer getWriteLatency() {
    return writeLatency;
  }

  /**
   * Gets commit latency.
   *
   * @return the commit latency
   */
  public OperationTimer getCommitLatency() {
    return commitLatency;
  }

  /**
   * Gets rollback count.
   *
   * @return the rollback count
   */
  public Counter getRollbackCount() {
    return rollbackCount;
  }
}
