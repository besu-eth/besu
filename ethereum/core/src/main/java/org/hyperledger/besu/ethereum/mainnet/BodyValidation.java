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

import static org.hyperledger.besu.crypto.Hash.keccak256;
import static org.hyperledger.besu.crypto.Hash.sha256;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.encoding.WithdrawalEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/** A utility class for body validation tasks. */
public final class BodyValidation {

  private BodyValidation() {
    // Utility Class
  }

  /**
   * Generates the transaction root for a list of transactions
   *
   * @param transactions the transactions
   * @return the transaction root
   */
  public static Hash transactionsRoot(final List<Transaction> transactions) {
    final ArrayList<Bytes> bytesList = new ArrayList<>(transactions.size());
    transactions.forEach(
        t -> bytesList.add(TransactionEncoder.encodeOpaqueBytes(t, EncodingContext.BLOCK_BODY)));

    return Util.getRootFromListOfBytes(bytesList);
  }

  /**
   * Generates the withdrawals root for a list of withdrawals
   *
   * @param withdrawals the transactions
   * @return the transaction root
   */
  public static Hash withdrawalsRoot(final List<Withdrawal> withdrawals) {
    final ArrayList<Bytes> bytesList = new ArrayList<>(withdrawals.size());
    withdrawals.forEach(w -> bytesList.add(WithdrawalEncoder.encodeOpaqueBytes(w)));

    return Util.getRootFromListOfBytes(bytesList);
  }

  /**
   * Generates the requests hash for a list of requests
   *
   * @param requests list of request (must be sorted by request type ascending)
   * @return the requests hash
   */
  public static Hash requestsHash(final List<Request> requests) {
    List<Bytes> requestHashes = new ArrayList<>();
    requests.forEach(
        request -> {
          // empty requests are excluded from the hash
          if (!request.getData().isEmpty()) {
            requestHashes.add(sha256(request.getEncodedRequest()));
          }
        });

    return Hash.wrap(sha256(Bytes.wrap(requestHashes)));
  }

  /**
   * Generates the receipt root for a list of receipts
   *
   * @param receipts the receipts
   * @return the receipt root
   */
  public static Hash receiptsRoot(final List<TransactionReceipt> receipts) {
    final ArrayList<Bytes> bytesList = new ArrayList<>(receipts.size());
    receipts.forEach(
        receipt ->
            bytesList.add(
                RLP.encode(
                    rlpOutput ->
                        TransactionReceiptEncoder.writeTo(
                            receipt,
                            rlpOutput,
                            TransactionReceiptEncodingConfiguration.TRIE_ROOT))));

    return Util.getRootFromListOfBytes(bytesList);
  }

  /**
   * Generates the ommers hash for a list of ommer block headers
   *
   * @param ommers the ommer block headers
   * @return the ommers hash
   */
  public static Hash ommersHash(final List<BlockHeader> ommers) {
    return Hash.wrap(keccak256(RLP.encode(out -> out.writeList(ommers, BlockHeader::writeTo))));
  }

  /**
   * Generates the logs bloom filter for a list of transaction receipts
   *
   * @param receipts the transaction receipts
   * @return the logs bloom filter
   */
  public static LogsBloomFilter logsBloom(final List<TransactionReceipt> receipts) {
    final LogsBloomFilter.Builder filterBuilder = LogsBloomFilter.builder();

    receipts.forEach(receipt -> filterBuilder.insertFilter(receipt.getBloomFilter()));

    return filterBuilder.build();
  }

  /**
   * Generates the block access list hash.
   *
   * @param bal the block access list
   * @return the block access list hash
   */
  public static Hash balHash(final BlockAccessList bal) {
    return Hash.wrap(keccak256(RLP.encode(bal::writeTo)));
  }

  /**
   * Generates the block access list hash from an already RLP-encoded block access list.
   *
   * @param balRlp the RLP-encoded block access list
   * @return the block access list hash
   */
  public static Hash balHash(final Bytes balRlp) {
    return Hash.wrap(keccak256(balRlp));
  }

  /**
   * Generates the block access list hash from a sync block access list holder.
   *
   * @param syncBlockAccessList the sync block access list
   * @return the block access list hash
   */
  public static Hash balHash(final SyncBlockAccessList syncBlockAccessList) {
    return balHash(syncBlockAccessList.getRlp());
  }
}
