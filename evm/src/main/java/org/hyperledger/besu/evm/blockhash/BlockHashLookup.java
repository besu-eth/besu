/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.blockhash;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Function that gets the block hash, passed in as part of TxValues.
 *
 * <p>Arg is the current executing message frame. The Result is the Hash, which may be zero based on
 * lookup rules.
 */
public interface BlockHashLookup extends BiFunction<MessageFrame, Long, Hash> {

  /**
   * How far back from the current block are hash queries valid for? Default is 256.
   *
   * @return The number of blocks before the current that should return a hash value.
   */
  default long getLookback() {
    return 256L;
  }

  /**
   * Lookup to use for one parallel worker (e.g. parallel transaction execution). Each worker must
   * hold an instance that does not share mutable traversal state with other workers.
   *
   * @return a {@link BlockHashLookup} safe to use from a single parallel worker thread
   * @throws UnsupportedOperationException if this lookup does not support parallel forking
   */
  default BlockHashLookup forkForParallelWorker() {
    throw new UnsupportedOperationException(
        "This BlockHashLookup does not support parallel execution; "
            + "override forkForParallelWorker() with a per-worker instance when used for parallel "
            + "transaction processing.");
  }

  /**
   * Ancestor block numbers and hashes this lookup resolved while processing the current block.
   *
   * <p>Not limited to numbers a {@code BLOCKHASH} opcode asked for. An implementation may seed the
   * parent hash up front and may cache every ancestor it walks past on the way to a requested one,
   * so the map is an upper bound on what execution observed rather than an exact record of it. *
   *
   * @return unmodifiable view of accessed ancestors keyed by block number, empty if unsupported
   */
  default Map<Long, Hash> getAccessedAncestors() {
    return Map.of();
  }
}
