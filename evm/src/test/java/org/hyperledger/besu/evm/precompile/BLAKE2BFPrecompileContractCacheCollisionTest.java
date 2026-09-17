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
package org.hyperledger.besu.evm.precompile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.precompile.AbstractPrecompiledContract.CacheEvent;
import org.hyperledger.besu.evm.precompile.AbstractPrecompiledContract.CacheMetric;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test asserting that two distinct, valid, zero-round BLAKE2F inputs whose leading 213
 * bytes previously shared a single 32-bit {@code Arrays.hashCode} value: {@code 31*0 + 0 == 31*1 +
 * (byte) 0xe1}. Alternating them evicted/replaced the single-entry hash bucket on every call,
 * causing deterministic cache thrashing and log amplification.
 */
class BLAKE2BFPrecompileContractCacheCollisionTest {

  private final PrecompiledContract contract =
      EvmSpec.evmSpec(EvmSpecVersion.ISTANBUL)
          .getPrecompileContractRegistry()
          .get(Address.BLAKE2B_F_COMPRESSION);

  private final MessageFrame messageFrame = mock(MessageFrame.class);

  // 213 zero bytes: 0 rounds, all-zero h/m/t, finalization flag 0. A valid zero-round call.
  private static final Bytes INPUT_A = Bytes.wrap(new byte[213]);

  // Differs from INPUT_A only at bytes 20 and 21 (0x01, 0xe1). Rounds (bytes 0-3) and the
  // finalization flag (byte 212) stay zero, so this is also a valid zero-round call.
  private static final Bytes INPUT_B;

  static {
    final MutableBytes mutable = MutableBytes.create(213);
    mutable.set(20, (byte) 0x01);
    mutable.set(21, (byte) 0xe1);
    INPUT_B = mutable.copy();
  }

  @BeforeEach
  void enableCaching() {
    AbstractPrecompiledContract.setPrecompileCaching(true);
  }

  @AfterEach
  void resetCaching() {
    AbstractPrecompiledContract.setPrecompileCaching(false);
    AbstractPrecompiledContract.setCacheEventConsumer(__ -> {});
  }

  @Test
  void collidingHashPrefixesNoLongerShareACacheBucket() {
    assertThat(AbstractPrecompiledContract.getCacheKey(INPUT_A, 213))
        .isNotEqualTo(AbstractPrecompiledContract.getCacheKey(INPUT_B, 213));
  }

  @Test
  void alternatingTheCollidingPairProducesHitsNotFalsePositives() {
    // Warm both entries first.
    contract.computePrecompile(INPUT_A, messageFrame);
    contract.computePrecompile(INPUT_B, messageFrame);

    final List<CacheEvent> events = new ArrayList<>();
    AbstractPrecompiledContract.setCacheEventConsumer(events::add);

    for (int i = 0; i < 10; i++) {
      contract.computePrecompile(INPUT_A, messageFrame);
      contract.computePrecompile(INPUT_B, messageFrame);
    }

    assertThat(events).extracting(CacheEvent::cacheMetric).containsOnly(CacheMetric.HIT);
  }
}
