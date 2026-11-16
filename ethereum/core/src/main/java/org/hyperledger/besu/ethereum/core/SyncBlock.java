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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.Objects;

public class SyncBlock {

  private final BlockHeader header;
  private final SyncBlockBody body;

  public SyncBlock(final BlockHeader header, final SyncBlockBody body) {
    this.header = header;
    this.body = body;
  }

  /**
   * Converts a block to its sync form by encoding the body and reading it back.
   *
   * @param block the block
   * @param protocolSchedule the protocol schedule used to decode the body
   * @return the sync block
   */
  public static SyncBlock of(final Block block, final ProtocolSchedule protocolSchedule) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    block.getBody().writeWrappedBodyTo(output);
    final SyncBlockBody body =
        SyncBlockBody.readWrappedBodyFrom(
            new BytesValueRLPInput(output.encoded(), false), false, protocolSchedule);
    return new SyncBlock(block.getHeader(), body);
  }

  public BlockHeader getHeader() {
    return header;
  }

  public SyncBlockBody getBody() {
    return body;
  }

  public Hash getHash() {
    return header.getHash();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof SyncBlock other)) {
      return false;
    }
    return header.equals(other.header) && body.equals(other.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(header, body);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Block{");
    sb.append("header=").append(header).append(", ");
    sb.append("body=").append(body);
    return sb.append("}").toString();
  }

  public String toLogString() {
    return getHeader().toLogString();
  }
}
