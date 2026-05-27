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
 * input, so bytes past the semantic length cannot grow the cache without bound.
 */
class PrecompileCachePrefixBoundTest {

  private static final String CANONICAL_ECREC_INPUT =
      "0x0049872459827432342344987245982743234234498724598274323423429943"
          + "000000000000000000000000000000000000000000000000000000000000001b"
          + "e8359c341771db7f9ea3a662a1741d27775ce277961470028e054ed3285aab8e"
          + "31f63eaac35c4e6178abbc2a1073040ac9bbb0b67f2bc89a2e9593ba9abe8c53";

  // Known-good G1 addition vector from the BLS12 test corpus.
  private static final String CANONICAL_G1ADD_INPUT =
      "0x0000000000000000000000000000000012196c5a43d69224d8713389285f26b9"
          + "8f86ee910ab3dd668e413738282003cc5b7357af9a7af54bb713d62255e80f56"
          + "0000000000000000000000000000000006ba8102bfbeea4416b710c73e8cce30"
          + "32c31c6269c44906f8ac4f7874ce99fb17559992486528963884ce429a992fee"
          + "000000000000000000000000000000000001101098f5c39893765766af4512a0"
          + "c74e1bb89bc7e6fdf14e3e7337d257cc0f94658179d83320b99f31ff94cd2bac"
          + "0000000000000000000000000000000003e1a9f9f44ca2cdab4f43a1a3ee3470"
          + "fdf90b2fc228eb3b709fcd72f014838ac82a6d797aeefed9a0804b22ed1ce8f7";

  private final MessageFrame messageFrame = mock(MessageFrame.class);

  @BeforeEach
  void enableCaching() {
    AbstractPrecompiledContract.setPrecompileCaching(true);
    AbstractBLS12PrecompiledContract.setPrecompileCaching(true);
  }

  @AfterEach
  void resetCaching() {
    AbstractPrecompiledContract.setPrecompileCaching(false);
    AbstractBLS12PrecompiledContract.setPrecompileCaching(false);
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

    final Bytes canonical = uniquePerRun(Bytes.fromHexString(CANONICAL_ECREC_INPUT));
    final Bytes tailPadded = Bytes.concatenate(canonical, Bytes.repeat((byte) 0xff, 1_600_000));

    final Bytes firstResult = contract.computePrecompile(canonical, messageFrame).output();
    final Bytes secondResult = contract.computePrecompile(tailPadded, messageFrame).output();

    assertThat(secondResult).isEqualTo(firstResult);
    assertThat(events)
        .extracting(CacheEvent::cacheMetric)
        .containsExactly(CacheMetric.MISS, CacheMetric.HIT);
  }

  @Test
  void bls12G1AddTailPaddedInputHitsCacheAndReturnsSameResult() {
    final BLS12G1AddPrecompiledContract contract = new BLS12G1AddPrecompiledContract();
    final List<CacheEvent> events = new ArrayList<>();
    AbstractPrecompiledContract.setCacheEventConsumer(events::add);

    // The BLS12 abstract uses inputLimit = paramLen + 1 = 257, so include the sentinel byte
    // in the canonical input. The cache prefix covers the first 257 bytes.
    final Bytes canonical =
        uniquePerRun(
            Bytes.concatenate(
                Bytes.fromHexString(CANONICAL_G1ADD_INPUT), Bytes.repeat((byte) 0x00, 1)));
    final Bytes tailPadded = Bytes.concatenate(canonical, Bytes.repeat((byte) 0xff, 1_000_000));

    final var firstResult = contract.computePrecompile(canonical, messageFrame);
    final var secondResult = contract.computePrecompile(tailPadded, messageFrame);

    assertThat(secondResult.output()).isEqualTo(firstResult.output());
    assertThat(events)
        .extracting(CacheEvent::cacheMetric)
        .containsExactly(CacheMetric.MISS, CacheMetric.HIT);
  }

  /**
   * XOR a per-run nonce into the leading byte so each test invocation uses a distinct cache key,
   * sidestepping shared static cache state across tests/JVM reuse.
   */
  private static Bytes uniquePerRun(final Bytes input) {
    final MutableBytes mutated = input.mutableCopy();
    mutated.set(0, (byte) (mutated.get(0) ^ (byte) System.nanoTime()));
    return mutated;
  }
}
