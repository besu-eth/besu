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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.plugin.Unstable;

/** A typed activation point for a protocol-schedule change. */
@Unstable
public sealed interface ProtocolScheduleActivation {

  /** Returns the block number or timestamp value in its declared domain. */
  long value();

  /** Creates a block-number activation. */
  static ProtocolScheduleActivation blockNumber(final long blockNumber) {
    return new BlockNumber(blockNumber);
  }

  /** Creates a timestamp activation. */
  static ProtocolScheduleActivation timestamp(final long timestamp) {
    return new Timestamp(timestamp);
  }

  /** A block-number activation. */
  record BlockNumber(long value) implements ProtocolScheduleActivation {
    @SuppressWarnings(
        "MethodInputParametersMustBeFinal") // compact record constructors have implicit parameters
    public BlockNumber {
      if (value < 0) {
        throw new IllegalArgumentException("Block-number activations must not be negative");
      }
    }
  }

  /** A timestamp activation. */
  record Timestamp(long value) implements ProtocolScheduleActivation {
    @SuppressWarnings(
        "MethodInputParametersMustBeFinal") // compact record constructors have implicit parameters
    public Timestamp {
      if (value < 0) {
        throw new IllegalArgumentException("Timestamp activations must not be negative");
      }
    }
  }
}
