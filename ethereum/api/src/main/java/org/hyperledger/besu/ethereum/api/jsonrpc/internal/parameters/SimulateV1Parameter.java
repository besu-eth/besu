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

import org.hyperledger.besu.ethereum.transaction.BlockSimulationParameter;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SimulateV1Parameter extends BlockSimulationParameter {

  private final boolean traceBlockImport;

  @JsonCreator
  public SimulateV1Parameter(
      @JsonProperty("blockStateCalls") final List<JsonBlockStateCallParameter> blockStateCalls,
      @JsonProperty("validation") final boolean validation,
      @JsonProperty("traceTransfers") final boolean traceTransfers,
      @JsonProperty("returnFullTransactions") final boolean returnFullTransactions,
      @JsonProperty("returnTrieLog") final boolean returnTrieLog,
      @JsonProperty("traceBlockImport") final boolean traceBlockImport) {
    super(blockStateCalls, validation, traceTransfers, returnFullTransactions, returnTrieLog);
    this.traceBlockImport = traceBlockImport;
  }

  public boolean isTraceBlockImport() {
    return traceBlockImport;
  }
}
