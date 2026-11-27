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
package org.hyperledger.besu.evm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.ChainIdOperation;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.OperationRegistry;
import org.hyperledger.besu.evm.operation.SStoreOperation;
import org.hyperledger.besu.evm.testutils.TestEvmConfigurations;
import org.hyperledger.besu.evm.v2.operation.AbstractOperationV2;
import org.hyperledger.besu.evm.v2.operation.ChainIdOperationV2;
import org.hyperledger.besu.evm.v2.operation.InvalidOperationV2;
import org.hyperledger.besu.evm.v2.operation.SStoreOperationV2;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

class MainnetEVMsOperationsV2Test {

  static final List<Function<EvmConfiguration, EVM>> FORKS =
      List.of(
          MainnetEVMs::frontier,
          MainnetEVMs::homestead,
          MainnetEVMs::spuriousDragon,
          MainnetEVMs::tangerineWhistle,
          MainnetEVMs::byzantium,
          MainnetEVMs::constantinople,
          MainnetEVMs::petersburg,
          MainnetEVMs::istanbul,
          MainnetEVMs::berlin,
          MainnetEVMs::london,
          MainnetEVMs::paris,
          MainnetEVMs::shanghai,
          MainnetEVMs::cancun,
          MainnetEVMs::prague,
          c -> MainnetEVMs.osaka(BigInteger.ONE, c),
          MainnetEVMs::amsterdam,
          MainnetEVMs::bogota,
          MainnetEVMs::polis,
          MainnetEVMs::bangkok,
          MainnetEVMs::futureEips,
          MainnetEVMs::experimentalEips);

  @ParameterizedTest
  @FieldSource("FORKS")
  void registersAV2OperationForEveryV1Operation(final Function<EvmConfiguration, EVM> fork) {
    final EVM evm = fork.apply(TestEvmConfigurations.EVM_V2);
    final Operation[] v1 = evm.getOperationsUnsafe();
    final Operation[] v2 = evm.getOperationsV2();

    for (int opcode = 0; opcode < 256; opcode++) {
      final String at = String.format("%s opcode 0x%02x", evm.getEvmVersion(), opcode);
      if (v1[opcode] == null) {
        assertThat(v2[opcode]).as(at).isNull();
        continue;
      }
      assertThat(v2[opcode]).as(at).isInstanceOf(AbstractOperationV2.class);
      assertThat(v2[opcode] instanceof InvalidOperationV2)
          .as(at)
          .isEqualTo(v1[opcode] instanceof InvalidOperation);
      assertThat(v2[opcode].getOpcode()).as(at).isEqualTo(v1[opcode].getOpcode());
      assertThat(v2[opcode].getName()).as(at).isEqualTo(v1[opcode].getName());
      if (v1[opcode] instanceof ChainIdOperation chainId) {
        assertThat(((ChainIdOperationV2) v2[opcode]).getChainId())
            .as(at)
            .isEqualTo(Bytes32.leftPad(chainId.getChainId()));
      }
      if (v1[opcode] instanceof SStoreOperation sStore) {
        assertThat(((SStoreOperationV2) v2[opcode]).getMinimumGasRemaining())
            .as(at)
            .isEqualTo(sStore.getMinimumGasRemaining());
      }
    }
  }

  @Test
  void takesTheChainIdOfTheFork() {
    final EVM evm = MainnetEVMs.cancun(BigInteger.valueOf(0x1234), TestEvmConfigurations.EVM_V2);

    final Operation chainId = evm.getOperationsV2()[ChainIdOperationV2.OPCODE];

    assertThat(((ChainIdOperationV2) chainId).getChainId())
        .isEqualTo(Bytes32.fromHexString("0x1234"));
  }

  @Test
  void buildsNoV2OperationsWhenTheConfigurationDisablesEvmV2() {
    assertThat(MainnetEVMs.cancun(TestEvmConfigurations.EVM_V1).getOperationsV2()).isNull();
  }

  @Test
  void rejectsEvmV2WithoutV2Operations() {
    assertThatThrownBy(
            () ->
                new EVM(
                    new OperationRegistry(),
                    new FrontierGasCalculator(),
                    TestEvmConfigurations.EVM_V2,
                    EvmSpecVersion.FRONTIER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FRONTIER");
  }
}
