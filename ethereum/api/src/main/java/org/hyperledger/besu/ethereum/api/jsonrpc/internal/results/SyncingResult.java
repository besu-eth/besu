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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.plugin.data.SyncStatus;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"startingBlock", "currentBlock", "highestBlock"})
public class SyncingResult implements JsonRpcResult {

  private final String startingBlock;
  private final String currentBlock;
  private final String highestBlock;
  private final String pullStates;
  private final String knownStates;

  public SyncingResult(final SyncStatus syncStatus) {

    this.startingBlock = Quantity.create(syncStatus.getStartingBlock());
    this.currentBlock = Quantity.create(syncStatus.getCurrentBlock());
    this.highestBlock = Quantity.create(syncStatus.getHighestBlock());
    this.pullStates = syncStatus.getPulledStates().map(Quantity::create).orElse(null);
    this.knownStates = syncStatus.getKnownStates().map(Quantity::create).orElse(null);
  }

  @JsonGetter(value = "startingBlock")
  public String getStartingBlock() {
    return startingBlock;
  }

  @JsonGetter(value = "currentBlock")
  public String getCurrentBlock() {
    return currentBlock;
  }

  @JsonGetter(value = "highestBlock")
  public String getHighestBlock() {
    return highestBlock;
  }

  @JsonInclude(value = Include.NON_NULL)
  @JsonGetter(value = "pulledStates")
  public String getPullStates() {
    return pullStates;
  }

  @JsonInclude(value = Include.NON_NULL)
  @JsonGetter(value = "knownStates")
  public String getKnownStates() {
    return knownStates;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SyncingResult that)) {
      return false;
    }
    return this.startingBlock.equals(that.startingBlock)
        && this.currentBlock.equals(that.currentBlock)
        && this.highestBlock.equals(that.highestBlock);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startingBlock, currentBlock, highestBlock);
  }
}
