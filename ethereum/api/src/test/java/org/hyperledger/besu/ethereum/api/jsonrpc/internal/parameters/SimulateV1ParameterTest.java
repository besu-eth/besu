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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class SimulateV1ParameterTest {

  private static final Set<Address> VALID_PRECOMPILE_ADDRESSES =
      Set.of(Address.fromHexString("0x0A"));
  private static final Address INVALID_PRECOMPILE_ADDRESS = Address.fromHexString("0x0B");

  private void validateSimulateV1Parameter(
      final List<JsonBlockStateCallParameter> blockStateCalls,
      final BlockStateCallError expectedError) {
    SimulateV1Parameter simulateV1Parameter =
        new SimulateV1Parameter(blockStateCalls, false, false, false, false, false);
    Optional<BlockStateCallError> maybeValidationError =
        simulateV1Parameter.validate(VALID_PRECOMPILE_ADDRESSES);
    assertThat(maybeValidationError).isPresent();
    assertThat(maybeValidationError.get()).isEqualTo(expectedError);
  }

  @Test
  public void shouldFailWhenBlockNumbersAreNotAscending() {
    JsonBlockStateCallParameter blockStateCall1 = createBlockStateCallParameter(2L, 1000L, null);
    JsonBlockStateCallParameter blockStateCall2 = createBlockStateCallParameter(1L, 1012L, null);

    validateSimulateV1Parameter(
        List.of(blockStateCall1, blockStateCall2), BlockStateCallError.BLOCK_NUMBERS_NOT_ASCENDING);
  }

  @Test
  public void shouldFailWhenTimestampsAreNotAscending() {
    JsonBlockStateCallParameter blockStateCall1 = createBlockStateCallParameter(0L, 1012L, null);
    JsonBlockStateCallParameter blockStateCall2 = createBlockStateCallParameter(1L, 1000L, null);

    validateSimulateV1Parameter(
        List.of(blockStateCall1, blockStateCall2), BlockStateCallError.TIMESTAMPS_NOT_ASCENDING);
  }

  @Test
  public void shouldFailWhenPrecompileAddressIsInvalid() {
    StateOverride stateOverride =
        StateOverride.builder().withMovePrecompileToAddress(Address.ZERO).build();
    StateOverrideMap stateOverrideMap = new StateOverrideMap();
    stateOverrideMap.put(INVALID_PRECOMPILE_ADDRESS, stateOverride);

    JsonBlockStateCallParameter blockStateCall =
        createBlockStateCallParameter(1L, 1000L, stateOverrideMap);

    validateSimulateV1Parameter(
        List.of(blockStateCall), BlockStateCallError.INVALID_PRECOMPILE_ADDRESS);
  }

  private JsonBlockStateCallParameter createBlockStateCallParameter(
      final Long blockNumber, final Long timestamp, final StateOverrideMap stateOverrideMap) {

    BlockOverridesParameter blockOverridesParameter =
        new BlockOverridesParameter(
            Optional.of(new UnsignedLongParameter(timestamp)),
            Optional.of(new UnsignedLongParameter(blockNumber)),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return new JsonBlockStateCallParameter(List.of(), blockOverridesParameter, stateOverrideMap);
  }
}
