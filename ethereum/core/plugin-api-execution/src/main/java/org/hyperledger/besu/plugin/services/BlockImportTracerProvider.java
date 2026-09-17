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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

/** The Block import tracer provider. */
@FunctionalInterface
public interface BlockImportTracerProvider extends BesuService {

  /**
   * Gets BlockAwareOperationTracer for use during block import
   *
   * @param blockHeader header of the block which will be imported
   * @return the block aware operation tracer
   */
  BlockAwareOperationTracer getBlockImportTracer(BlockHeader blockHeader);
}
