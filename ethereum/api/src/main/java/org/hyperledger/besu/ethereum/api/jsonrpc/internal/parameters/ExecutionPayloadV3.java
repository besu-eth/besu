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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.core.json.QuantityJson;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "parentHash",
  "feeRecipient",
  "stateRoot",
  "receiptsRoot",
  "logsBloom",
  "prevRandao",
  "blockNumber",
  "gasLimit",
  "gasUsed",
  "timestamp",
  "extraData",
  "baseFeePerGas",
  "blockHash",
  "transactions",
  "withdrawals",
  "blobGasUsed",
  "excessBlobGas"
})
public sealed class ExecutionPayloadV3 extends ExecutionPayloadV2 permits ExecutionPayloadV4 {
  private Long blobGasUsed;
  private BlobGas excessBlobGas;

  public ExecutionPayloadV3() {}

  public ExecutionPayloadV3(
      final BlockHeader header,
      final List<Transaction> transactions,
      final List<Withdrawal> withdrawals) {
    super(header, transactions, withdrawals);
    this.blobGasUsed = header.getBlobGasUsed().orElse(0L);
    this.excessBlobGas = header.getExcessBlobGas().orElse(BlobGas.ZERO);
  }

  @JsonSetter("blobGasUsed")
  @JsonDeserialize(using = QuantityJson.LongDeserializer.class)
  public void setBlobGasUsed(final Long blobGasUsed) {
    this.blobGasUsed = blobGasUsed;
  }

  @JsonSetter("excessBlobGas")
  public void setExcessBlobGas(final BlobGas excessBlobGas) {
    this.excessBlobGas = excessBlobGas;
  }

  @JsonSerialize(using = QuantityJson.LongSerializer.class)
  public Long getBlobGasUsed() {
    return blobGasUsed;
  }

  public BlobGas getExcessBlobGas() {
    return excessBlobGas;
  }
}
