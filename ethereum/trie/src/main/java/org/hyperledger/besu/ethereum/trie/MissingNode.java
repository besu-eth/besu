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
package org.hyperledger.besu.ethereum.trie;

public class MissingNode<V> extends NullNode<V> {

  private final byte[] hash;
  private final byte[] location;
  private final byte[] path;

  public MissingNode(final byte[] hash, final byte[] location) {
    this.hash = hash;
    this.location = location;
    this.path =
        location.length == 0 ? Nibbles.EMPTY : Nibbles.slice(location, 0, location.length - 1);
  }

  @Override
  public byte[] hash() {
    return hash;
  }

  @Override
  public byte[] path() {
    return path;
  }

  @Override
  public boolean isHealNeeded() {
    return true;
  }

  @Override
  public byte[] location() {
    return location;
  }
}
