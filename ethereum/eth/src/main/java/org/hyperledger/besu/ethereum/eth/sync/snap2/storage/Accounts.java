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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;

import org.apache.tuweni.bytes.Bytes;

/** Encoding of the flat accounts, which carry their storage root. */
public final class Accounts {

  /** Storage root of an account whose slots changed once its storage was downloaded. */
  public static final Hash UNKNOWN_STORAGE_ROOT = Hash.ZERO;

  private Accounts() {}

  public static PatriciaTrieAccountValue decode(final Bytes rlp) {
    return PatriciaTrieAccountValue.readFrom(RLP.input(rlp));
  }

  public static Bytes encode(final PatriciaTrieAccountValue account) {
    return RLP.encode(account::writeTo);
  }

  public static PatriciaTrieAccountValue withStorageRoot(
      final PatriciaTrieAccountValue account, final Hash storageRoot) {
    return new PatriciaTrieAccountValue(
        account.getNonce(), account.getBalance(), storageRoot, account.getCodeHash());
  }

  /** Returns whether a storage trie can be built for this root: neither empty nor unknown. */
  public static boolean hasBuildableStorage(final Hash storageRoot) {
    return !storageRoot.equals(Hash.EMPTY_TRIE_HASH) && !storageRoot.equals(UNKNOWN_STORAGE_ROOT);
  }

  /** Returns whether the account is empty in the EIP-161 sense. */
  public static boolean isEmpty(final PatriciaTrieAccountValue account) {
    return account.getNonce() == 0
        && account.getBalance().isZero()
        && account.getCodeHash().equals(Hash.EMPTY);
  }
}
