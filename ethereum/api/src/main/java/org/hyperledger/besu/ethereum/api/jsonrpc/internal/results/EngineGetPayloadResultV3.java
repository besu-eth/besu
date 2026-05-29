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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"executionPayload", "blockValue", "blobsBundle", "shouldOverrideBuilder"})
public class EngineGetPayloadResultV3 {
  protected final ExecutionPayloadV3 executionPayload;
  private final String blockValue;
  private final BlobsBundleV1 blobsBundle;
  private final boolean shouldOverrideBuilder;

  public EngineGetPayloadResultV3(
      final BlockHeader header,
      final List<Transaction> transactions,
      final List<Withdrawal> withdrawals,
      final String blockValue,
      final BlobsBundleV1 blobsBundle) {
    this.executionPayload = new ExecutionPayloadV3(header, transactions, withdrawals);
    this.blockValue = blockValue;
    this.blobsBundle = blobsBundle;
    this.shouldOverrideBuilder = false;
  }

  @JsonGetter(value = "executionPayload")
  public ExecutionPayloadV3 getExecutionPayload() {
    return executionPayload;
  }

  @JsonGetter(value = "blockValue")
  public String getBlockValue() {
    return blockValue;
  }

  @JsonGetter(value = "blobsBundle")
  public BlobsBundleV1 getBlobsBundle() {
    return blobsBundle;
  }

  @JsonGetter(value = "shouldOverrideBuilder")
  public boolean shouldOverrideBuilder() {
    return shouldOverrideBuilder;
  }
}
