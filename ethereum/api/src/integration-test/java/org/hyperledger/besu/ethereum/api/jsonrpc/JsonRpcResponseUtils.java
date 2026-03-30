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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.BASEFEE;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.COINBASE;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.DIFFICULTY;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.EXTRA_DATA;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.GAS_LIMIT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.GAS_USED;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.LOGS_BLOOM;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.MIX_HASH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.NONCE;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.NUMBER;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.OMMERS_HASH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.PARENT_HASH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.RECEIPTS_ROOT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.REQUESTS_HASH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.SIZE;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.STATE_ROOT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.TIMESTAMP;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.TOTAL_DIFFICULTY;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.TRANSACTION_ROOT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcResponseKey.WITHDRAWALS_ROOT;

import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionHashResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionWithMetadataResult;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class JsonRpcResponseUtils {

  /** Hex is base 16 */
  private static final int HEX_RADIX = 16;

  /**
   * @param values hex encoded values.
   */
  public JsonRpcResponse response(final Map<JsonRpcResponseKey, String> values) {
    return response(values, new ArrayList<>());
  }

  /**
   * @param values hex encoded values.
   */
  public JsonRpcResponse response(
      final Map<JsonRpcResponseKey, String> values, final List<TransactionResult> transactions) {

    final Hash mixHash = hash(values.get(MIX_HASH));
    final Hash parentHash = hash(values.get(PARENT_HASH));
    final Hash ommersHash = hash(values.get(OMMERS_HASH));
    final Address coinbase = address(values.get(COINBASE));
    final Hash stateRoot = hash(values.get(STATE_ROOT));
    final Hash transactionsRoot = hash(values.get(TRANSACTION_ROOT));
    final Hash receiptsRoot = hash(values.get(RECEIPTS_ROOT));
    final LogsBloomFilter logsBloom = logsBloom(values.get(LOGS_BLOOM));
    final Difficulty difficulty = Difficulty.of(unsignedInt256(values.get(DIFFICULTY)));
    final Bytes extraData = bytes(values.get(EXTRA_DATA));
    final BlockHeaderFunctions blockHeaderFunctions = new MainnetBlockHeaderFunctions();
    final long number = unsignedLong(values.get(NUMBER));
    final long gasLimit = unsignedLong(values.get(GAS_LIMIT));
    final long gasUsed = unsignedLong(values.get(GAS_USED));
    final long timestamp = unsignedLong(values.get(TIMESTAMP));
    final long nonce = unsignedLong(values.get(NONCE));
    final Wei baseFee =
        values.containsKey(BASEFEE) ? Wei.of(unsignedInt256(values.get(BASEFEE))) : null;
    final Difficulty totalDifficulty = Difficulty.of(unsignedInt256(values.get(TOTAL_DIFFICULTY)));
    final int size = unsignedInt(values.get(SIZE));
    final Hash withdrawalsRoot =
        values.containsKey(WITHDRAWALS_ROOT) ? hash(values.get(WITHDRAWALS_ROOT)) : null;
    final Hash requestsHash =
        values.containsKey(REQUESTS_HASH) ? hash(values.get(REQUESTS_HASH)) : null;
    final List<JsonNode> ommers = new ArrayList<>();

    final BlockHeader header =
        new BlockHeader(
            parentHash,
            ommersHash,
            coinbase,
            stateRoot,
            transactionsRoot,
            receiptsRoot,
            logsBloom,
            difficulty,
            number,
            gasLimit,
            gasUsed,
            timestamp,
            extraData,
            baseFee,
            Bytes32.wrap(mixHash.getBytes()),
            nonce,
            withdrawalsRoot,
            null, // ToDo 4844: set with the value of blob_gas_used field
            null, // ToDo 4844: set with the value of excess_blob_gas field
            null, // TODO 4788: set with the value of the parent beacon block root field
            requestsHash,
            null, // block access list hash
            null, // slotNumber
            blockHeaderFunctions);

    return new JsonRpcSuccessResponse(
        null, new BlockResult(header, transactions, ommers, totalDifficulty, size));
  }

  public List<TransactionResult> transactions(final String... values) {
    final List<TransactionResult> nodes = new ArrayList<>(values.length);

    for (int i = 0; i < values.length; i++) {
      nodes.add(new TransactionHashResult(values[i]));
    }

    return nodes;
  }

  public List<TransactionResult> transactions(final TransactionResult... transactions) {
    final List<TransactionResult> list = new ArrayList<>(transactions.length);

    for (final TransactionResult transaction : transactions) {
      list.add(transaction);
    }

    return list;
  }

  public TransactionResult transaction(
      final TransactionType transactionType,
      final String blockHash,
      final String blockNumber,
      final Wei baseFee,
      final String fromAddress,
      final String gas,
      final String gasPrice,
      final String hash,
      final String input,
      final String nonce,
      final String publicKey,
      final String raw,
      final String toAddress,
      final String transactionIndex,
      final String value,
      final String v,
      final String r,
      final String s,
      final String blockTimestamp) {

    final Transaction transaction =
        Transaction.builder()
            .type(transactionType)
            .nonce(unsignedLong(nonce))
            .gasPrice(Wei.fromHexString(gasPrice))
            .gasLimit(unsignedLong(gas))
            .to(address(toAddress))
            .value(wei(value))
            .signature(
                SignatureAlgorithmFactory.getInstance()
                    .createSignature(
                        Bytes.fromHexString(r).toUnsignedBigInteger(),
                        Bytes.fromHexString(s).toUnsignedBigInteger(),
                        Bytes.fromHexString(v)
                            .toUnsignedBigInteger()
                            .subtract(Transaction.REPLAY_UNPROTECTED_V_BASE)
                            .byteValueExact()))
            .payload(bytes(input))
            .sender(address(fromAddress))
            .build();

    return new TransactionWithMetadataResult(
        new TransactionWithMetadata(
            transaction,
            unsignedLong(blockNumber),
            Optional.ofNullable(baseFee),
            Hash.fromHexString(blockHash),
            unsignedInt(transactionIndex),
            unsignedLong(blockTimestamp)));
  }

  private int unsignedInt(final String value) {
    final String hex = removeHexPrefix(value);
    return new BigInteger(hex, HEX_RADIX).intValue();
  }

  private long unsignedLong(final String value) {
    final String hex = removeHexPrefix(value);
    return new BigInteger(hex, HEX_RADIX).longValue();
  }

  private Hash hash(final String hex) {
    return Hash.fromHexString(hex);
  }

  private String removeHexPrefix(final String prefixedHex) {
    return prefixedHex.startsWith("0x") ? prefixedHex.substring(2) : prefixedHex;
  }

  private Wei wei(final String hex) {
    return Wei.fromHexString(hex);
  }

  private Address address(final String hex) {
    return Address.fromHexString(hex);
  }

  private LogsBloomFilter logsBloom(final String hex) {
    return LogsBloomFilter.fromHexString(hex);
  }

  private UInt256 unsignedInt256(final String hex) {
    return UInt256.fromHexString(hex);
  }

  private Bytes bytes(final String hex) {
    return Bytes.fromHexString(hex);
  }
}
