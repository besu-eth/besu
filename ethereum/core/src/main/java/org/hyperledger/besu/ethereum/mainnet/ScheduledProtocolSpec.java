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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;

/**
 * Associates a {@link ProtocolSpec} with a given block number or timestamp level starting point
 * Knows how to query the timestamp or block number of a given block header
 */
public interface ScheduledProtocolSpec {

  boolean isOnOrAfterMilestoneBoundary(
      org.hyperledger.besu.plugin.data.ProcessableBlockHeader header);

  boolean isOnMilestoneBoundary(org.hyperledger.besu.plugin.data.ProcessableBlockHeader header);

  Hardfork fork();

  ProtocolSpec spec();

  public record Hardfork(String name, long milestone) implements Comparable<Hardfork> {
    @Override
    public int compareTo(final Hardfork h) {
      if (h == null) { // all non-null hardforks are greater than null
        return 1;
      }
      return this.milestone == h.milestone ? 0 : this.milestone < h.milestone ? -1 : 1;
    }

    @Override
    public String toString() {
      return String.format("%s:%d", name, milestone);
    }
  }

  class TimestampProtocolSpec implements ScheduledProtocolSpec {

    private final long timestamp;
    private final ProtocolSpec protocolSpec;

    public static TimestampProtocolSpec create(
        final long timestamp, final ProtocolSpec protocolSpec) {
      return new TimestampProtocolSpec(timestamp, protocolSpec);
    }

    private TimestampProtocolSpec(final long timestamp, final ProtocolSpec protocolSpec) {
      this.timestamp = timestamp;
      this.protocolSpec = protocolSpec;
    }

    @Override
    public boolean isOnOrAfterMilestoneBoundary(
        final org.hyperledger.besu.plugin.data.ProcessableBlockHeader header) {
      return Long.compareUnsigned(header.getTimestamp(), timestamp) >= 0;
    }

    @Override
    public boolean isOnMilestoneBoundary(
        final org.hyperledger.besu.plugin.data.ProcessableBlockHeader header) {
      return header.getTimestamp() == timestamp;
    }

    @Override
    public Hardfork fork() {
      return new Hardfork(protocolSpec.getHardforkId().description(), timestamp);
    }

    @Override
    public ProtocolSpec spec() {
      return protocolSpec;
    }
  }

  class BlockNumberProtocolSpec implements ScheduledProtocolSpec {
    private final long blockNumber;
    private final ProtocolSpec protocolSpec;

    public static BlockNumberProtocolSpec create(
        final long blockNumber, final ProtocolSpec protocolSpec) {
      return new BlockNumberProtocolSpec(blockNumber, protocolSpec);
    }

    private BlockNumberProtocolSpec(final long blockNumber, final ProtocolSpec protocolSpec) {
      this.blockNumber = blockNumber;
      this.protocolSpec = protocolSpec;
    }

    @Override
    public boolean isOnOrAfterMilestoneBoundary(
        final org.hyperledger.besu.plugin.data.ProcessableBlockHeader header) {
      return header.getNumber() >= blockNumber;
    }

    @Override
    public boolean isOnMilestoneBoundary(final ProcessableBlockHeader header) {
      return header.getNumber() == blockNumber;
    }

    @Override
    public Hardfork fork() {
      return new Hardfork(protocolSpec.getHardforkId().description(), blockNumber);
    }

    @Override
    public ProtocolSpec spec() {
      return protocolSpec;
    }
  }
}
