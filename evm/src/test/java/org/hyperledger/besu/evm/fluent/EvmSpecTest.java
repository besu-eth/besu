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
package org.hyperledger.besu.evm.fluent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class EvmSpecTest {
  @SuppressWarnings({"removal", "InlineMeInliner"})
  @Test
  void defaultChainIdAPIs() {
    Bytes32 defaultChainId = Bytes32.leftPad(Bytes.of(1));

    EvmSpec istanbulEVM = EvmSpec.istanbul(EvmConfiguration.DEFAULT);
    assertThat(istanbulEVM.getChainId()).contains(defaultChainId);

    EvmSpec berlinEVM = EvmSpec.berlin(EvmConfiguration.DEFAULT);
    assertThat(berlinEVM.getChainId()).contains(defaultChainId);

    EvmSpec londonEVM = EvmSpec.london(EvmConfiguration.DEFAULT);
    assertThat(londonEVM.getChainId()).contains(defaultChainId);

    EvmSpec parisEVM = EvmSpec.paris(EvmConfiguration.DEFAULT);
    assertThat(parisEVM.getChainId()).contains(defaultChainId);

    EvmSpec shanghaiEVM = EvmSpec.shanghai(EvmConfiguration.DEFAULT);
    assertThat(shanghaiEVM.getChainId()).contains(defaultChainId);

    EvmSpec cancunEVM = EvmSpec.cancun(EvmConfiguration.DEFAULT);
    assertThat(cancunEVM.getChainId()).contains(defaultChainId);

    EvmSpec pragueEVM = EvmSpec.prague(defaultChainId.toBigInteger(), EvmConfiguration.DEFAULT);
    assertThat(pragueEVM.getChainId()).contains(defaultChainId);

    EvmSpec osakaEVM = EvmSpec.osaka(defaultChainId.toBigInteger(), EvmConfiguration.DEFAULT);
    assertThat(osakaEVM.getChainId()).contains(defaultChainId);

    EvmSpec futureEipsVM = EvmSpec.futureEips(EvmConfiguration.DEFAULT);
    assertThat(futureEipsVM.getChainId()).contains(defaultChainId);
  }

  @Test
  void nullEvmSpec() {
    assertThrows(NullPointerException.class, () -> new EVMExecutor(EvmSpec.evmSpec((EVM) null)));
  }

  @Test
  void currentEVM() {
    var subject = EvmSpec.evmSpec();
    assertThat(subject.getEVMVersion()).isEqualTo(EvmSpecVersion.AMSTERDAM);
  }

  @ParameterizedTest
  @EnumSource(EvmSpecVersion.class)
  void evmByRequest(final EvmSpecVersion version) {
    var subject = EvmSpec.evmSpec(version);
    assertThat(subject.getEVMVersion()).isEqualTo(version);
  }

  @ParameterizedTest
  @EnumSource(EvmSpecVersion.class)
  void evmWithChainIDByRequest(final EvmSpecVersion version) {
    var subject = EvmSpec.evmSpec(version, BigInteger.TEN);
    assertThat(subject.getEVMVersion()).isEqualTo(version);
    if (EvmSpecVersion.ISTANBUL.compareTo(version) <= 0) {
      assertThat(subject.getChainId()).map(Bytes::trimLeadingZeros).map(Bytes::toInt).contains(10);
    } else {
      assertThat(subject.getChainId()).isEmpty();
    }
  }

  @ParameterizedTest
  @EnumSource(EvmSpecVersion.class)
  void evmWithChainIDByBytes(final EvmSpecVersion version) {
    var subject = EvmSpec.evmSpec(version, Bytes.fromHexString("0xc4a1201d"));
    assertThat(subject.getEVMVersion()).isEqualTo(version);
    if (EvmSpecVersion.ISTANBUL.compareTo(version) <= 0) {
      assertThat(subject.getChainId())
          .map(Bytes::trimLeadingZeros)
          .map(Bytes::toInt)
          .contains(0xc4a1201d);
    } else {
      assertThat(subject.getChainId()).isEmpty();
    }
  }
}
