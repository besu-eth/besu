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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.datatypes.BytesHolder;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.bytes.Bytes;

@JsonPropertyOrder({
  "accessList",
  "authorizationList",
  "blockHash",
  "blockNumber",
  "blockTimestamp",
  "chainId",
  "from",
  "gas",
  "gasPrice",
  "maxPriorityFeePerGas",
  "maxFeePerGas",
  "maxFeePerBlobGas",
  "hash",
  "input",
  "nonce",
  "to",
  "transactionIndex",
  "type",
  "value",
  "yParity",
  "v",
  "r",
  "s",
  "blobVersionedHashes"
})
public class TransactionBaseResult implements TransactionResult {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final List<AccessListEntry> accessList;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String chainId;

  private final String from;
  private final String gas;
  private final String gasPrice;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String maxPriorityFeePerGas;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String maxFeePerGas;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String maxFeePerBlobGas;

  private final String hash;
  private final String input;
  private final String nonce;
  private final String to;
  private final String type;
  private final String value;
  private final String yParity;
  private final String v;
  private final String r;
  private final String s;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final List<String> versionedHashes;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final List<CodeDelegationResult> authorizationList;

  public TransactionBaseResult(final Transaction transaction) {
    this(transaction, Optional.empty());
  }

  public TransactionBaseResult(final Transaction transaction, final Optional<Wei> maybeBaseFee) {
    final TransactionType transactionType = transaction.getType();
    this.accessList =
        transaction.getAccessList().orElse(transactionType.supportsAccessList() ? List.of() : null);
    this.chainId = transaction.getChainId().map(Quantity::create).orElse(null);
    this.from = transaction.getSender().toString();
    this.gas = Quantity.create(transaction.getGasLimit());
    this.maxPriorityFeePerGas =
        transaction.getMaxPriorityFeePerGas().map(Wei::toShortHexString).orElse(null);
    this.maxFeePerGas = transaction.getMaxFeePerGas().map(Wei::toShortHexString).orElse(null);
    this.maxFeePerBlobGas =
        transaction.getMaxFeePerBlobGas().map(Wei::toShortHexString).orElse(null);
    this.gasPrice =
        Quantity.create(
            transaction
                .getGasPrice()
                .orElseGet(
                    () ->
                        maybeBaseFee.isPresent()
                            ? transaction.getEffectiveGasPrice(maybeBaseFee)
                            : transaction.getMaxFeePerGas().get()));
    this.hash = transaction.getHash().toString();
    this.input = transaction.getPayload().toString();
    this.nonce = Quantity.create(transaction.getNonce());
    this.to = transaction.getTo().map(a -> a.getBytes().toHexString()).orElse(null);
    if (transactionType == TransactionType.FRONTIER) {
      this.type = Quantity.create(0);
      this.yParity = null;
      this.v = Quantity.create(transaction.getV());
    } else {
      this.type = Quantity.create(transactionType.getSerializedType());
      this.yParity = Quantity.create(transaction.getYParity());
      this.v =
          (transactionType == TransactionType.ACCESS_LIST
                  || transactionType == TransactionType.EIP1559
                  || transactionType == TransactionType.DELEGATE_CODE
                  || transactionType == TransactionType.BLOB)
              ? Quantity.create(transaction.getYParity())
              : null;
    }
    this.value = Quantity.create(transaction.getValue());
    this.r = Quantity.create(transaction.getR());
    this.s = Quantity.create(transaction.getS());
    this.versionedHashes =
        transaction
            .getVersionedHashes()
            .map(
                hashes ->
                    hashes.stream().map(BytesHolder::getBytes).map(Bytes::toHexString).toList())
            .orElse(null);
    this.authorizationList =
        transaction
            .getCodeDelegationList()
            .map(cds -> cds.stream().map(CodeDelegationResult::new).toList())
            .orElse(null);
  }

  @JsonGetter(value = "accessList")
  public List<AccessListEntry> getAccessList() {
    return accessList;
  }

  @JsonGetter(value = "chainId")
  public String getChainId() {
    return chainId;
  }

  @JsonGetter(value = "from")
  public String getFrom() {
    return from;
  }

  @JsonGetter(value = "gas")
  public String getGas() {
    return gas;
  }

  @JsonGetter(value = "gasPrice")
  public String getGasPrice() {
    return gasPrice;
  }

  @JsonGetter(value = "maxPriorityFeePerGas")
  public String getMaxPriorityFeePerGas() {
    return maxPriorityFeePerGas;
  }

  @JsonGetter(value = "maxFeePerGas")
  public String getMaxFeePerGas() {
    return maxFeePerGas;
  }

  @JsonGetter(value = "maxFeePerBlobGas")
  public String getMaxFeePerBlobGas() {
    return maxFeePerBlobGas;
  }

  @JsonGetter(value = "hash")
  public String getHash() {
    return hash;
  }

  @JsonGetter(value = "input")
  public String getInput() {
    return input;
  }

  @JsonGetter(value = "nonce")
  public String getNonce() {
    return nonce;
  }

  @JsonGetter(value = "to")
  public String getTo() {
    return to;
  }

  @JsonGetter(value = "type")
  public String getType() {
    return type;
  }

  @JsonGetter(value = "value")
  public String getValue() {
    return value;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonGetter(value = "yParity")
  public String getYParity() {
    return yParity;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonGetter(value = "v")
  public String getV() {
    return v;
  }

  @JsonGetter(value = "r")
  public String getR() {
    return r;
  }

  @JsonGetter(value = "s")
  public String getS() {
    return s;
  }

  @JsonGetter(value = "blobVersionedHashes")
  public List<String> getVersionedHashes() {
    return versionedHashes;
  }

  @JsonGetter(value = "authorizationList")
  public List<CodeDelegationResult> getAuthorizationList() {
    return authorizationList;
  }

  @JsonGetter(value = "blockHash")
  public String getBlockHash() {
    return null;
  }

  @JsonGetter(value = "blockNumber")
  public String getBlockNumber() {
    return null;
  }

  @JsonGetter(value = "blockTimestamp")
  public String getBlockTimestamp() {
    return null;
  }

  @JsonGetter(value = "transactionIndex")
  public String getTransactionIndex() {
    return null;
  }
}
