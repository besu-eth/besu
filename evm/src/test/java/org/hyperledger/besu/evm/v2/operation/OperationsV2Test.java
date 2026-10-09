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
package org.hyperledger.besu.evm.v2.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.AbstractOperation;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.operation.Operation;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

class OperationsV2Test {

  static final List<Function<EvmConfiguration, EVM>> FORKS =
      List.of(
          MainnetEVMs::frontier,
          MainnetEVMs::homestead,
          MainnetEVMs::byzantium,
          MainnetEVMs::constantinople,
          MainnetEVMs::istanbul,
          MainnetEVMs::london,
          MainnetEVMs::paris,
          MainnetEVMs::shanghai,
          MainnetEVMs::cancun,
          MainnetEVMs::prague,
          c -> MainnetEVMs.osaka(BigInteger.ONE, c),
          MainnetEVMs::amsterdam,
          MainnetEVMs::bogota,
          MainnetEVMs::futureEips,
          MainnetEVMs::experimentalEips);

  @ParameterizedTest
  @FieldSource("FORKS")
  void everyOpcodeOfTheForkHasAV2Operation(final Function<EvmConfiguration, EVM> fork) {
    final EVM evm = fork.apply(EvmConfiguration.DEFAULT);
    final Operation[] v1 = evm.getOperationsUnsafe();

    final Operation[] v2 = operationsV2(evm);

    for (int opcode = 0; opcode < 256; opcode++) {
      if (v1[opcode] instanceof InvalidOperation) {
        assertThat(v2[opcode]).as("opcode 0x%02x", opcode).isSameAs(v1[opcode]);
      } else if (v2[opcode] != null) {
        assertThat(v2[opcode]).as("opcode 0x%02x", opcode).isInstanceOf(AbstractOperationV2.class);
        assertThat(v2[opcode].getOpcode()).isEqualTo(opcode);
        assertThat(v2[opcode].getName()).isEqualTo(v1[opcode].getName());
      }
    }
  }

  @Test
  void takesTheChainIdFromTheRegistry() {
    final EVM evm = MainnetEVMs.cancun(BigInteger.valueOf(0x1234), EvmConfiguration.DEFAULT);

    final Operation chainId = operationsV2(evm)[ChainIdOperationV2.OPCODE];

    assertThat(((ChainIdOperationV2) chainId).getChainId())
        .isEqualTo(Bytes32.fromHexString("0x1234"));
  }

  @Test
  void takesTheSStoreMinimumGasFromTheRegistry() {
    assertThat(sStore(MainnetEVMs.frontier(EvmConfiguration.DEFAULT)).getMinimumGasRemaining())
        .isZero();
    assertThat(sStore(MainnetEVMs.istanbul(EvmConfiguration.DEFAULT)).getMinimumGasRemaining())
        .isEqualTo(2300L);
  }

  @Test
  void hasPayOnlyWhereTheForkRegistersIt() {
    assertThat(operationsV2(MainnetEVMs.amsterdam(EvmConfiguration.DEFAULT))[0xfc])
        .isInstanceOf(InvalidOperation.class);
    assertThat(operationsV2(MainnetEVMs.futureEips(EvmConfiguration.DEFAULT))[0xfc])
        .isInstanceOf(PayOperationV2.class);
  }

  @Test
  void rejectsAnOperationWithoutAV2Version() {
    final GasCalculator gasCalculator = new FrontierGasCalculator();
    final Operation[] operations = new Operation[256];
    operations[0x0c] =
        new AbstractOperation(0x0c, "CUSTOM", 0, 0, gasCalculator) {
          @Override
          public OperationResult execute(final MessageFrame frame, final EVM evm) {
            return new OperationResult(0, null);
          }
        };

    assertThatThrownBy(() -> OperationsV2.of(operations, gasCalculator, EvmSpecVersion.FRONTIER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("CUSTOM");
  }

  private static SStoreOperationV2 sStore(final EVM evm) {
    return (SStoreOperationV2) operationsV2(evm)[0x55];
  }

  private static Operation[] operationsV2(final EVM evm) {
    return OperationsV2.of(evm.getOperationsUnsafe(), evm.getGasCalculator(), evm.getEvmVersion());
  }
}
