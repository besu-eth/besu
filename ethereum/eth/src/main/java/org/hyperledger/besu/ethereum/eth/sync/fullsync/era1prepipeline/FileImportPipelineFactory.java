/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.fullsync.era1prepipeline;

import org.hyperledger.besu.services.pipeline.Pipeline;

/** A factory for constructing Pipeline objects handling file import */
public interface FileImportPipelineFactory extends AutoCloseable {
  /**
   * Creates a FileImportPipeline starting from the supplied currentHeadBlockNumber + 1
   *
   * @param currentHeadBlockNumber The block number of the current head
   * @return a FileImportPipeline starting from the supplied currentHeadBlockNumber + 1
   */
  Pipeline<?> createFileImportPipelineForCurrentBlockNumber(final long currentHeadBlockNumber);

  @Override
  default void close() {}
}
