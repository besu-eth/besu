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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.json.QuantityJson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonPropertyOrder({"terminalTotalDifficulty", "terminalBlockHash", "terminalBlockNumber"})
public class TransitionConfigurationV1 {
  private final Difficulty terminalTotalDifficulty;
  private final Hash terminalBlockHash;
  private final long terminalBlockNumber;

  @JsonCreator
  public TransitionConfigurationV1(
      @JsonProperty("terminalTotalDifficulty") final Difficulty terminalTotalDifficulty,
      @JsonProperty("terminalBlockHash") final Hash terminalBlockHash,
      @JsonProperty("terminalBlockNumber")
          @JsonDeserialize(using = QuantityJson.LongDeserializer.class)
          final long terminalBlockNumber) {
    this.terminalTotalDifficulty = terminalTotalDifficulty;
    this.terminalBlockHash = terminalBlockHash;
    this.terminalBlockNumber = terminalBlockNumber;
  }

  public Difficulty getTerminalTotalDifficulty() {
    return terminalTotalDifficulty;
  }

  public Hash getTerminalBlockHash() {
    return terminalBlockHash;
  }

  @JsonSerialize(using = QuantityJson.LongSerializer.class)
  public long getTerminalBlockNumber() {
    return terminalBlockNumber;
  }
}
