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
package org.hyperledger.besu.ethereum.trie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * This exception is thrown when there is an issue retrieving or decoding values from {@link
 * MerkleStorage}.
 */
public class MerkleTrieException extends RuntimeException {

  private Optional<Address> maybeAddress;
  private Bytes32 hash;
  private Bytes location;

  public MerkleTrieException(final String message) {
    super(message);
  }

  public MerkleTrieException(final String message, final Bytes32 hash, final Bytes location) {
    super(message);
    this.hash = hash;
    this.location = location;
    this.maybeAddress = Optional.empty();
  }

  public MerkleTrieException(final String message, final Exception cause) {
    super(message, cause);
  }

  public MerkleTrieException(
      final String message,
      final Optional<Address> maybeAddress,
      final Bytes32 hash,
      final Bytes location) {
    super(message);
    this.hash = hash;
    this.location = location;
    this.maybeAddress = maybeAddress;
  }

  /**
   * Checks that the code of a non-empty code hash was found. Meant to be called from an {@code
   * assert}: it throws instead of returning false, so with assertions enabled the failure is a
   * local storage error rather than an {@link AssertionError}.
   *
   * @param address the account the code belongs to
   * @param codeHash the account's code hash
   * @param found whether the code was found in storage
   * @return always true
   * @throws MerkleTrieException if the code hash is non-empty and the code was not found
   */
  public static boolean checkCodeFound(
      final Address address, final Hash codeHash, final boolean found) {
    if (!found && !Hash.EMPTY.equals(codeHash)) {
      throw new MerkleTrieException(
          "invalid account code",
          Optional.of(address),
          Bytes32.wrap(codeHash.getBytes()),
          Bytes.EMPTY);
    }
    return true;
  }

  public Optional<Address> getMaybeAddress() {
    return maybeAddress;
  }

  public Bytes32 getHash() {
    return hash;
  }

  public Bytes getLocation() {
    return location;
  }
}
