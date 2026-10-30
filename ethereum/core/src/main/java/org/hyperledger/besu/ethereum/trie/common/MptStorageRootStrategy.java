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
package org.hyperledger.besu.ethereum.trie.common;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

/**
 * Merkle Patricia trie storage root strategy. Owns a mutable storage root; the account RLP is the
 * 4-field {@code [nonce, balance, storageRoot, codeHash]}.
 */
public final class MptStorageRootStrategy implements StorageRootStrategy {

  private Hash storageRoot;

  public MptStorageRootStrategy(final Hash storageRoot) {
    this.storageRoot = storageRoot;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MptStorageRootStrategy other)) {
      return false;
    }
    return Objects.equals(storageRoot, other.storageRoot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageRoot);
  }

  @Override
  public Hash getStorageRoot() {
    return storageRoot;
  }

  @Override
  public void setStorageRoot(final Hash root) {
    this.storageRoot = root;
  }

  @Override
  public void writeStorageRoot(final RLPOutput out) {
    out.writeBytes(storageRoot.getBytes());
  }

  @Override
  public void readStorageRoot(final RLPInput in) {
    this.storageRoot = Hash.wrap(in.readBytes32());
  }

  @Override
  public void assertStorageRootMatches(final AccountValue other, final String context) {
    if (!Objects.equals(storageRoot, other.getStorageRoot())) {
      throw new IllegalStateException(context + ": Storage Roots differ");
    }
  }

  @Override
  public StorageRootStrategy copy() {
    return new MptStorageRootStrategy(storageRoot);
  }
}
