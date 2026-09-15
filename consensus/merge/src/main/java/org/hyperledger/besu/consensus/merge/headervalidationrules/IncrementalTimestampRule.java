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
package org.hyperledger.besu.consensus.merge.headervalidationrules;

import static org.hyperledger.besu.consensus.merge.TransitionUtils.isTerminalProofOfWorkBlock;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Incremental timestamp rule. */
public class IncrementalTimestampRule extends MergeConsensusRule {

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalTimestampRule.class);

  /** Default Constructor. */
  public IncrementalTimestampRule() {}

  @Override
  public boolean validate(
      final BlockHeader header, final BlockHeader parent, final ProtocolContext protocolContext) {

    if (super.shouldUsePostMergeRules(header, protocolContext)
        && !isTerminalProofOfWorkBlock(header, protocolContext)) {
      final long blockTimestamp = header.getTimestamp();
      final long parentTimestamp = parent.getTimestamp();
      // Both timestamps are uint64 carried in longs, so they must be compared unsigned: a value
      // above Long.MAX_VALUE is negative and would otherwise look older than its parent.
      final boolean isMoreRecent = Long.compareUnsigned(blockTimestamp, parentTimestamp) > 0;

      LOG.trace(
          "Is block timestamp more recent that its parent? {}, [block timestamp {}, parent timestamp {}]",
          isMoreRecent,
          blockTimestamp,
          parentTimestamp);

      return isMoreRecent;
    } else {
      return true;
    }
  }

  @Override
  public String toString() {
    return "MergeIncrementalTimestamp";
  }
}
