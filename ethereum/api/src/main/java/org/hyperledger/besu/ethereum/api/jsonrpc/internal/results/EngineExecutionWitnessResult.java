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

import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.tuweni.bytes.Bytes;

/**
 * RLP-encoded execution witness for {@code engine_newPayloadWithWitnessV5} (EIP-8025).
 *
 * <p>Serializes as a hex-prefixed RLP byte string. The encoding is {@code RLP([headers, codes,
 * state])} where each field is a list of byte arrays.
 *
 * @see ExecutionWitnessResult for the JSON object variant used by {@code debug_executionWitness}.
 */
public class EngineExecutionWitnessResult {

  private final String hexValue;

  public EngineExecutionWitnessResult(
      final List<String> state, final List<String> codes, final List<String> headers) {
    this.hexValue =
        RLP.encode(
                out -> {
                  out.startList();
                  // Headers are already RLP-encoded lists
                  out.writeList(headers, (item, o) -> o.writeRLPBytes(Bytes.fromHexString(item)));
                  out.writeList(codes, (item, o) -> o.writeBytes(Bytes.fromHexString(item)));
                  out.writeList(state, (item, o) -> o.writeBytes(Bytes.fromHexString(item)));
                  out.endList();
                })
            .toHexString();
  }

  @JsonValue
  public String getValue() {
    return hexValue;
  }
}
