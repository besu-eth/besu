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

import java.util.Objects;

public class SyncBlock {

  private final BlockHeader header;
  private final SyncBlockBody body;

  public SyncBlock(final BlockHeader header, final SyncBlockBody body) {
    this.header = header;
    this.body = body;
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
