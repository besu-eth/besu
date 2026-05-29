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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({
  "executionPayload",
  "blockValue",
  "blobsBundle",
  "shouldOverrideBuilder",
  "executionRequests"
})
public class EngineGetPayloadResultV5 {
  protected final ExecutionPayloadV3 executionPayload;
  private final String blockValue;
  private final BlobsBundleV2 blobsBundle;
  private final boolean shouldOverrideBuilder;
  private final List<String> executionRequests;

  public EngineGetPayloadResultV5(
      final BlockHeader header,
      final List<Transaction> transactions,
      final Optional<List<Withdrawal>> withdrawals,
      final Optional<List<String>> executionRequests,
      final String blockValue,
      final BlobsBundleV2 blobsBundle) {
    this.executionPayload = new ExecutionPayloadV3(header, transactions, withdrawals);
    this.blockValue = blockValue;
    this.blobsBundle = blobsBundle;
    this.shouldOverrideBuilder = false;
    this.executionRequests = executionRequests.orElse(null);
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
  public BlobsBundleV2 getBlobsBundle() {
    return blobsBundle;
  }

  @JsonGetter(value = "shouldOverrideBuilder")
  public boolean shouldOverrideBuilder() {
    return shouldOverrideBuilder;
  }

  @JsonGetter(value = "executionRequests")
  public List<String> getExecutionRequests() {
    return executionRequests;
  }
}
