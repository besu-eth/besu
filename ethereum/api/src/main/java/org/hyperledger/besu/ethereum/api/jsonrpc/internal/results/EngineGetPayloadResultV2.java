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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.bytes.Bytes32;

@JsonPropertyOrder({
  "executionPayload",
  "blockValue",
})
public class EngineGetPayloadResultV2 {
  protected final PayloadResult executionPayload;
  private final String blockValue;

  public EngineGetPayloadResultV2(
      final BlockHeader header,
      final List<String> transactions,
      final Optional<List<Withdrawal>> withdrawals,
      final String blockValue) {
    this.executionPayload = new PayloadResult(header, transactions, withdrawals);
    this.blockValue = blockValue;
  }

  @JsonGetter(value = "executionPayload")
  public PayloadResult getExecutionPayload() {
    return executionPayload;
  }

  @JsonGetter(value = "blockValue")
  public String getBlockValue() {
    return blockValue;
  }

  public static class PayloadResult {

    protected final String blockHash;
    private final String parentHash;
    private final String feeRecipient;
    private final String stateRoot;
    private final String receiptsRoot;
    private final String logsBloom;
    private final String prevRandao;
    private final String blockNumber;
    private final String gasLimit;
    private final String gasUsed;
    private final String timestamp;
    private final String extraData;
    private final String baseFeePerGas;
    protected final List<String> transactions;
    private final List<WithdrawalParameter> withdrawals;

    public PayloadResult(
        final BlockHeader header,
        final List<String> transactions,
        final Optional<List<Withdrawal>> withdrawals) {
      this.blockNumber = Quantity.create(header.getNumber());
      this.blockHash = header.getHash().toString();
      this.parentHash = header.getParentHash().toString();
      this.logsBloom = header.getLogsBloom().toString();
      this.stateRoot = header.getStateRoot().toString();
      this.receiptsRoot = header.getReceiptsRoot().toString();
      this.extraData = header.getExtraData().toString();
      this.baseFeePerGas = header.getBaseFee().map(Quantity::create).orElse(null);
      this.gasLimit = Quantity.create(header.getGasLimit());
      this.gasUsed = Quantity.create(header.getGasUsed());
      this.timestamp = Quantity.create(header.getTimestamp());
      this.transactions = transactions;
      this.feeRecipient = header.getCoinbase().toChecksumString();
      this.prevRandao = header.getPrevRandao().map(Bytes32::toHexString).orElse(null);
      this.withdrawals =
          withdrawals
              .map(
                  ws ->
                      ws.stream()
                          .map(WithdrawalParameter::fromWithdrawal)
                          .collect(Collectors.toList()))
              .orElse(null);
    }

    @JsonGetter(value = "blockNumber")
    public String getNumber() {
      return blockNumber;
    }

    @JsonGetter(value = "blockHash")
    public String getHash() {
      return blockHash;
    }

    @JsonGetter(value = "parentHash")
    public String getParentHash() {
      return parentHash;
    }

    @JsonGetter(value = "logsBloom")
    public String getLogsBloom() {
      return logsBloom;
    }

    @JsonGetter(value = "prevRandao")
    public String getPrevRandao() {
      return prevRandao;
    }

    @JsonGetter(value = "stateRoot")
    public String getStateRoot() {
      return stateRoot;
    }

    @JsonGetter(value = "receiptsRoot")
    public String getReceiptRoot() {
      return receiptsRoot;
    }

    @JsonGetter(value = "extraData")
    public String getExtraData() {
      return extraData;
    }

    @JsonGetter(value = "baseFeePerGas")
    public String getBaseFeePerGas() {
      return baseFeePerGas;
    }

    @JsonGetter(value = "gasLimit")
    public String getGasLimit() {
      return gasLimit;
    }

    @JsonGetter(value = "gasUsed")
    public String getGasUsed() {
      return gasUsed;
    }

    @JsonGetter(value = "timestamp")
    public String getTimestamp() {
      return timestamp;
    }

    @JsonGetter(value = "transactions")
    public List<String> getTransactions() {
      return transactions;
    }

    @JsonGetter(value = "withdrawals")
    public List<WithdrawalParameter> getWithdrawals() {
      return withdrawals;
    }

    @JsonGetter(value = "feeRecipient")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getFeeRecipient() {
      return feeRecipient;
    }
  }
}
