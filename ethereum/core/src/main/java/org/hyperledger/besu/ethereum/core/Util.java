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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class Util {

  // A list this long, such as the transactions or receipts of a large block, is encoded and hashed
  // in parallel.
  private static final int MIN_ENTRIES_TO_HASH_IN_PARALLEL = 512;

  private static final int ENTRIES_PER_SUBTREE = 256;

  /**
   * Converts a Signature to an Address, underlying math requires the hash of the data used to
   * create the signature.
   *
   * @param seal the signature from which an address is to be extracted
   * @param dataHash the hash of the data which was signed.
   * @return The Address of the Ethereum node which signed the data defined by the supplied dataHash
   */
  public static Address signatureToAddress(final SECPSignature seal, final Hash dataHash) {
    return SignatureAlgorithmFactory.getInstance()
        .recoverPublicKeyFromSignature(Bytes32.wrap(dataHash.getBytes()), seal)
        .map(Util::publicKeyToAddress)
        .orElse(null);
  }

  public static Address publicKeyToAddress(final SECPPublicKey publicKey) {
    return publicKeyToAddress(publicKey.getEncodedBytes());
  }

  public static Address publicKeyToAddress(final Bytes publicKeyBytes) {
    return Address.extract(Bytes32.wrap(Hash.hash(publicKeyBytes).getBytes()));
  }

  /**
   * Implements a fast version of ceiling(numerator/denominator) that does not require using
   * floating point math
   *
   * @param numerator Numerator
   * @param denominator Denominator
   * @return result of ceiling(numerator/denominator)
   */
  public static int fastDivCeiling(final int numerator, final int denominator) {
    return ((numerator - 1) / denominator) + 1;
  }

  /**
   * RLP encoded index for an MPT based on the index.
   *
   * @param i index of the index key
   * @return the RLP encoded index key
   */
  public static Bytes indexKey(final int i) {
    return RLP.encodeOne(UInt256.valueOf(i).trimLeadingZeros());
  }

  /**
   * Calculates the root of an MPT based on it's entries, encoding them in parallel for a long list.
   *
   * @param entries list of the entries strictly ordered by index, starting at 0
   * @param encoder encodes an entry into its value in the MPT
   * @param <T> the type of the entries
   * @return the root hash of the MPT
   */
  public static <T> Hash getRootFromList(final List<T> entries, final Function<T, Bytes> encoder) {
    return getRootFromListOfBytes(
        (entries.size() >= MIN_ENTRIES_TO_HASH_IN_PARALLEL
                ? entries.parallelStream()
                : entries.stream())
            .map(encoder)
            .toList());
  }

  /**
   * Calculates the root of an MPT based on it's entries, which must not be empty.
   *
   * <p>The trie is built from its keys in ascending order, holding only the path still being
   * filled, rather than by inserting each entry from the root. The keys are the RLP-encoded
   * indices: 1 to 127 encode as a single byte, 0 as 0x80, and from 128 on they are length-prefixed
   * and increase with the index.
   *
   * @param bytes list of the entries strictly ordered by index, starting at 0
   * @return the root hash of the MPT
   */
  public static Hash getRootFromListOfBytes(final List<Bytes> bytes) {
    if (bytes.isEmpty()) {
      return Hash.EMPTY_TRIE_HASH;
    }
    final int size = bytes.size();
    final AscendingCollapsePatriciaTrie trie = new AscendingCollapsePatriciaTrie();
    for (int i = 1; i < Math.min(size, 0x80); i++) {
      trie.insert(indexKey(i), bytes.get(i));
    }
    trie.insert(indexKey(0), bytes.get(0));
    if (size < MIN_ENTRIES_TO_HASH_IN_PARALLEL) {
      for (int i = 0x80; i < size; i++) {
        trie.insert(indexKey(i), bytes.get(i));
      }
    } else {
      for (int i = 0x80; i < Math.min(size, ENTRIES_PER_SUBTREE); i++) {
        trie.insert(indexKey(i), bytes.get(i));
      }
      // From 256 on, the keys of each run of 256 indices differ only in their last byte, so each
      // run is a complete subtree that can be built and hashed on its own.
      IntStream.iterate(
              ENTRIES_PER_SUBTREE, start -> start < size, start -> start + ENTRIES_PER_SUBTREE)
          .boxed()
          .toList()
          .parallelStream()
          .map(
              start ->
                  prepareSubtree(trie, bytes, start, Math.min(size, start + ENTRIES_PER_SUBTREE)))
          .toList()
          .forEach(trie::insert);
    }
    return Hash.wrap(trie.rootHash());
  }

  private static AscendingCollapsePatriciaTrie.Subtree prepareSubtree(
      final AscendingCollapsePatriciaTrie trie,
      final List<Bytes> bytes,
      final int start,
      final int end) {
    final List<Bytes> keys = new ArrayList<>(end - start);
    for (int i = start; i < end; i++) {
      keys.add(indexKey(i));
    }
    return trie.prepare(keys, bytes.subList(start, end));
  }
}
