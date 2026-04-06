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
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

/**
 * Represents the "transaction" part of the JSON of a general state tests.
 *
 * <p>This contains information for a transaction, indirectly versioned by milestone. More
 * precisely, this there is 2 steps to transform this class (which again, represent what is in the
 * JSON) to an actual transaction to test:
 *
 * <ul>
 *   <li>in the state test json, gas, value and data for the transaction are arrays. This is how
 *       state tests deal with milestone versioning: for a given milestone, the actual value to use
 *       is defined by the indexes of the "post" section of the json. Those indexes are passed to
 *       this class in {@link #get(GeneralStateTestCaseSpec.Indexes)}.
 *   <li>the signature of the transaction is not provided in the json directly. Instead, the private
 *       key of the sender is provided, and the transaction must thus be signed (also in {@link
 *       #get(GeneralStateTestCaseSpec.Indexes)}) through {@link
 *       Transaction.Builder#signAndBuild(KeyPair)}.
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StateTestVersionedTransaction {

  private final long nonce;
  @Nullable private final Wei maxFeePerGas;
  @Nullable private final Wei maxPriorityFeePerGas;
  @Nullable private final Wei gasPrice;
  @Nullable private final Address to;

  private final KeyPair keys;

  private final List<Long> gasLimits;
  private final List<Wei> values;
  private final List<Bytes> payloads;
  private final Optional<List<List<AccessListEntry>>> maybeAccessLists;
  private final Wei maxFeePerBlobGas;
  // String instead of VersionedHash because reference tests intentionally use bad hashes.
  private final List<String> blobVersionedHashes;

  @JsonDeserialize(contentAs = org.hyperledger.besu.ethereum.core.CodeDelegation.class)
  private final List<org.hyperledger.besu.datatypes.CodeDelegation> authorizationList;

  /**
   * Constructor for populating a mock transaction with json data.
   *
   * @param nonce Nonce of the mock transaction.
   * @param gasPrice Gas price of the mock transaction, if not 1559 transaction.
   * @param maxFeePerGas Wei fee cap of the mock transaction, if a 1559 transaction.
   * @param maxPriorityFeePerGas Wei tip cap of the mock transaction, if a 1559 transaction.
   * @param gasLimit Gas Limit of the mock transaction.
   * @param to Recipient account of the mock transaction.
   * @param value Amount of ether transferred in the mock transaction.
   * @param secretKey Secret Key of the mock transaction.
   * @param data Call data of the mock transaction.
   * @param maybeAccessLists List of access lists of the mock transaction. Can be null.
   */
  @JsonCreator
  public StateTestVersionedTransaction(
      @JsonProperty("nonce") final String nonce,
      @JsonProperty("gasPrice") final String gasPrice,
      @JsonProperty("maxFeePerGas") final String maxFeePerGas,
      @JsonProperty("maxPriorityFeePerGas") final String maxPriorityFeePerGas,
      @JsonProperty("gasLimit") final String[] gasLimit,
      @JsonProperty("to") final String to,
      @JsonProperty("value") final String[] value,
      @JsonProperty("secretKey") final String secretKey,
      @JsonProperty("data") final String[] data,
      @JsonDeserialize(using = StateTestAccessListDeserializer.class) @JsonProperty("accessLists")
          final List<List<AccessListEntry>> maybeAccessLists,
      @JsonProperty("maxFeePerBlobGas") final String maxFeePerBlobGas,
      @JsonProperty("blobVersionedHashes") final List<String> blobVersionedHashes,
      @JsonProperty("authorizationList")
          final List<org.hyperledger.besu.datatypes.CodeDelegation> authorizationList) {

    this.nonce = Bytes.fromHexStringLenient(nonce).toLong();
    this.gasPrice = Optional.ofNullable(gasPrice).map(Wei::fromHexString).orElse(null);
    this.maxFeePerGas = Optional.ofNullable(maxFeePerGas).map(Wei::fromHexString).orElse(null);
    this.maxPriorityFeePerGas =
        Optional.ofNullable(maxPriorityFeePerGas).map(Wei::fromHexString).orElse(null);
    this.to = (to == null || to.isEmpty()) ? null : Address.fromHexString(to);

    SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    this.keys =
        signatureAlgorithm.createKeyPair(
            signatureAlgorithm.createPrivateKey(Bytes32.fromHexString(secretKey)));

    this.gasLimits = parseArray(gasLimit, s -> UInt256.fromHexString(s).toLong());
    this.values = parseArray(value, Wei::fromHexString);
    this.payloads = parseArray(data, Bytes::fromHexString);
    this.maybeAccessLists = Optional.ofNullable(maybeAccessLists);
    this.maxFeePerBlobGas =
        Optional.ofNullable(maxFeePerBlobGas).map(Wei::fromHexString).orElse(null);
    this.blobVersionedHashes = blobVersionedHashes;
    this.authorizationList = authorizationList;
  }

  private static <T> List<T> parseArray(final String[] array, final Function<String, T> parseFct) {
    if (array == null) {
      return null;
    }

    final List<T> res = new ArrayList<>(array.length);
    for (final String str : array) {
      try {
        res.add(parseFct.apply(str));
      } catch (RuntimeException re) {
        // the reference tests may be testing a boundary violation
        res.add(null);
      }
    }
    return res;
  }

  public Transaction get(final GeneralStateTestCaseSpec.Indexes indexes) {
    Long gasLimit = gasLimits.get(indexes.gas);
    Wei value = values.get(indexes.value);
    Bytes data = payloads.get(indexes.data);
    if (value == null || gasLimit == null) {
      // this means one of the params is an out-of-bounds value. Don't generate the transaction.
      return null;
    }

    final Transaction.Builder transactionBuilder =
        Transaction.builder().nonce(nonce).gasLimit(gasLimit).to(to).value(value).payload(data);

    Optional.ofNullable(gasPrice).ifPresent(transactionBuilder::gasPrice);
    Optional.ofNullable(maxFeePerGas).ifPresent(transactionBuilder::maxFeePerGas);
    Optional.ofNullable(maxPriorityFeePerGas).ifPresent(transactionBuilder::maxPriorityFeePerGas);
    maybeAccessLists.ifPresent(
        accessLists -> transactionBuilder.accessList(accessLists.get(indexes.data)));
    Optional.ofNullable(maxFeePerBlobGas).ifPresent(transactionBuilder::maxFeePerBlobGas);
    try {
      transactionBuilder.versionedHashes(
          blobVersionedHashes == null
              ? null
              : blobVersionedHashes.stream().map(VersionedHash::fromHexString).toList());
    } catch (IllegalArgumentException iae) {
      // versioned hash string was bad, so this is an invalid transaction
      return null;
    }
    Optional.ofNullable(authorizationList).ifPresent(transactionBuilder::codeDelegations);

    transactionBuilder.guessType();
    if (transactionBuilder.getTransactionType().requiresChainId()) {
      transactionBuilder.chainId(BigInteger.ONE);
    }

    return transactionBuilder.signAndBuild(keys);
  }
}
