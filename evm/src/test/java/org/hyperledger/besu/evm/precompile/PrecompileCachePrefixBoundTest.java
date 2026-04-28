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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.SpuriousDragonGasCalculator;
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
 * Verifies that precompile result caches key on the semantic-prefix slice rather than the full
 * input. This prevents an attacker from forcing unbounded retained allocations by appending tail
 * bytes that the precompile semantics ignore.
 */
class PrecompileCachePrefixBoundTest {

  private static final String CANONICAL_ECREC_INPUT =
      "0x0049872459827432342344987245982743234234498724598274323423429943"
          + "000000000000000000000000000000000000000000000000000000000000001b"
          + "e8359c341771db7f9ea3a662a1741d27775ce277961470028e054ed3285aab8e"
          + "31f63eaac35c4e6178abbc2a1073040ac9bbb0b67f2bc89a2e9593ba9abe8c53";

  private final MessageFrame messageFrame = mock(MessageFrame.class);

  @BeforeEach
  void enableCachingAndCaptureEvents() {
    AbstractPrecompiledContract.setPrecompileCaching(true);
  }

  @AfterEach
  void resetCaching() {
    AbstractPrecompiledContract.setPrecompileCaching(false);
    AbstractPrecompiledContract.setCacheEventConsumer(__ -> {});
  }

  @Test
  void getCacheKeyIgnoresBytesPastPrefix() {
    final Bytes prefix = Bytes.fromHexString(CANONICAL_ECREC_INPUT);
    final Bytes paddedShort = Bytes.concatenate(prefix, Bytes.repeat((byte) 0xab, 16));
    final Bytes paddedLong = Bytes.concatenate(prefix, MutableBytes.create(1_600_000));

    final int prefixKey = AbstractPrecompiledContract.getCacheKey(prefix, 128);
    final int paddedShortKey = AbstractPrecompiledContract.getCacheKey(paddedShort, 128);
    final int paddedLongKey = AbstractPrecompiledContract.getCacheKey(paddedLong, 128);

    assertThat(paddedShortKey).isEqualTo(prefixKey);
    assertThat(paddedLongKey).isEqualTo(prefixKey);
  }

  @Test
  void getCacheKeyDistinguishesDifferentPrefixes() {
    final Bytes a = Bytes.fromHexString(CANONICAL_ECREC_INPUT);
    final MutableBytes mutated = a.mutableCopy();
    mutated.set(0, (byte) (mutated.get(0) ^ 0x01));

    assertThat(AbstractPrecompiledContract.getCacheKey(mutated, 128))
        .isNotEqualTo(AbstractPrecompiledContract.getCacheKey(a, 128));
  }

  @Test
  void ecrecTailPaddedInputHitsCacheAndReturnsSameResult() {
    final ECRECPrecompiledContract contract =
        new ECRECPrecompiledContract(new SpuriousDragonGasCalculator());
    final List<CacheEvent> events = new ArrayList<>();
    AbstractPrecompiledContract.setCacheEventConsumer(events::add);

    final Bytes canonical = Bytes.fromHexString(CANONICAL_ECREC_INPUT);
    final Bytes tailPadded = Bytes.concatenate(canonical, Bytes.repeat((byte) 0xff, 1_600_000));

    final Bytes firstResult = contract.computePrecompile(canonical, messageFrame).output();
    final Bytes secondResult = contract.computePrecompile(tailPadded, messageFrame).output();

    assertThat(secondResult).isEqualTo(firstResult);
    assertThat(events)
        .extracting(CacheEvent::cacheMetric)
        .containsExactly(CacheMetric.MISS, CacheMetric.HIT);
  }
}
