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
package org.hyperledger.besu.evm.gascalculator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FrameTransactionGasTest {

  private final GasCalculator gasCalculator = new PragueGasCalculator();

  @Test
  void intrinsicGasCostSingleFrameWithSecp256k1() {
    // 1 frame, 1 SECP256K1 signature (2800 gas)
    long cost = gasCalculator.frameTransactionIntrinsicGasCost(1, 2800L);
    // 15000 + 475*1 + 2800 = 18275
    assertThat(cost).isEqualTo(18275L);
  }

  @Test
  void intrinsicGasCostMultipleFrames() {
    // 3 frames, 2 SECP256K1 signatures (5600 gas)
    long cost = gasCalculator.frameTransactionIntrinsicGasCost(3, 5600L);
    // 15000 + 475*3 + 5600 = 22025
    assertThat(cost).isEqualTo(22025L);
  }

  @Test
  void intrinsicGasCostWithP256() {
    // 2 frames, 1 P256 signature (6700 gas)
    long cost = gasCalculator.frameTransactionIntrinsicGasCost(2, 6700L);
    // 15000 + 475*2 + 6700 = 22650
    assertThat(cost).isEqualTo(22650L);
  }

  @Test
  void intrinsicGasCostMaxFrames() {
    // 64 frames (MAX_FRAMES), no signatures
    long cost = gasCalculator.frameTransactionIntrinsicGasCost(64, 0L);
    // 15000 + 475*64 + 0 = 45400
    assertThat(cost).isEqualTo(45400L);
  }

  @Test
  void constantsMatchSpec() {
    assertThat(GasCalculator.FRAME_TX_INTRINSIC_COST).isEqualTo(15_000L);
    assertThat(GasCalculator.FRAME_TX_PER_FRAME_COST).isEqualTo(475L);
  }
}
