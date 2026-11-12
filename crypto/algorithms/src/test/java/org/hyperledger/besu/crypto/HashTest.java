/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.crypto;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.AbstractBytes;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.Test;

public class HashTest {

  private static final String cowKeccak256 =
      "c85ef7d79691fe79573b1a7064c19c1a9819ebdbd1faaab1a8ec92344438aaf4";
  private static final String horseKeccak256 =
      "c87f65ff3f271bf5dc8643484f66b200109caffe4bf98c4cb393dc35740b28c0";

  private static final String cowSha256 =
      "beb134754910a4b4790c69ab17d3975221f4c534b70c8d6e82b30c165e8c0c09";
  private static final String horseSha256 =
      "fd62862b6dc213bee77c2badd6311528253c6cb3107e03c16051aa15584eca1c";

  private static final String cowRipemd160 = "7e72ea7f1d397cca847d1393f744536b2ce99693";
  private static final String horseRipemd160 = "79a19669bea55f3b2f67038155755fb0a68999ba";

  private static final String inputBlake2bf =
      "000000016a09e667f2bd8948bb67ae8584caa73b3c6ef372fe94f82ba54ff53a5f1d36f1510e527fade682d19b05688c2b3e6c1f1f83d9abfb41bd6b5be0cd19137e217907060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a191827262524232221202f2e2d2c2b2a292837363534333231303f3e3d3c3b3a3938000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000001";

  private static final String outputBlake2bf =
      "bd45dd07a6eac13eec520b6aac885a1a89ea882978ae1a07472cc3e8cd358fd1a4d431c1a7d03f4a2bf746506779a970bfc5477026701076eda181e16acc24bc";

  /** Validate keccak256 hash. */
  @Test
  public void keccak256Hash() {
    final Bytes resultHorse = Hash.keccak256(Bytes.wrap("horse".getBytes(UTF_8)));
    assertThat(resultHorse).isEqualTo(Bytes.fromHexString(horseKeccak256));

    final Bytes resultCow = Hash.keccak256(Bytes.wrap("cow".getBytes(UTF_8)));
    assertThat(resultCow).isEqualTo(Bytes.fromHexString(cowKeccak256));

    assertThat(Hash.keccak256(Bytes.wrap("horse".getBytes(UTF_8))))
        .isEqualTo(Bytes.fromHexString(horseKeccak256));
  }

  @Test
  public void keccak256HashIsCorrectAfterAnInputFailsHalfway() {
    final Bytes failing = new FailingHalfwayBytes(Bytes.wrap("horse".getBytes(UTF_8)));
    assertThatThrownBy(() -> Hash.keccak256(failing)).isInstanceOf(IllegalStateException.class);

    assertThat(Hash.keccak256(Bytes.wrap("cow".getBytes(UTF_8))))
        .isEqualTo(Bytes.fromHexString(cowKeccak256));
  }

  @Test
  public void keccak256HashIsCorrectOnConcurrentThreads() throws Exception {
    final Bytes horse = Bytes.wrap("horse".getBytes(UTF_8));
    final Bytes cow = Bytes.wrap("cow".getBytes(UTF_8));
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      final List<Future<Boolean>> results = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        final Bytes input = i % 2 == 0 ? horse : cow;
        final Bytes expected = Bytes.fromHexString(i % 2 == 0 ? horseKeccak256 : cowKeccak256);
        results.add(
            executor.submit(
                () -> {
                  for (int n = 0; n < 10_000; n++) {
                    if (!Hash.keccak256(input).equals(expected)) {
                      return false;
                    }
                  }
                  return true;
                }));
      }
      for (final Future<Boolean> result : results) {
        assertThat(result.get(1, TimeUnit.MINUTES)).isTrue();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /** Validate sha256 hash. */
  @Test
  public void sha256Hash() {
    final Bytes resultHorse = Hash.sha256(Bytes.wrap("horse".getBytes(UTF_8)));
    assertThat(resultHorse).isEqualTo(Bytes.fromHexString(horseSha256));

    final Bytes resultCow = Hash.sha256(Bytes.wrap("cow".getBytes(UTF_8)));
    assertThat(resultCow).isEqualTo(Bytes.fromHexString(cowSha256));
  }

  @Test
  public void sha256HashIsCorrectAfterAnInputFailsHalfway() {
    final Bytes failing = new FailingHalfwayBytes(Bytes.wrap("horse".getBytes(UTF_8)));
    assertThatThrownBy(() -> Hash.sha256(failing)).isInstanceOf(IllegalStateException.class);

    assertThat(Hash.sha256(Bytes.wrap("cow".getBytes(UTF_8))))
        .isEqualTo(Bytes.fromHexString(cowSha256));
  }

  /** Validate ripemd160 hash. */
  @Test
  public void ripemd160Hash() {
    final Bytes resultHorse = Hash.ripemd160(Bytes.wrap("horse".getBytes(UTF_8)));
    assertThat(resultHorse).isEqualTo(Bytes.fromHexString(horseRipemd160));

    final Bytes resultCow = Hash.ripemd160(Bytes.wrap("cow".getBytes(UTF_8)));
    assertThat(resultCow).isEqualTo(Bytes.fromHexString(cowRipemd160));
  }

  /** Validate blake2f compression digest. */
  @Test
  public void blake2bfCompression() {
    final Bytes result = Hash.blake2bf(Bytes.wrap(Hex.decode(inputBlake2bf)));
    assertThat(result).isEqualTo(Bytes.fromHexString(outputBlake2bf));
  }

  /** Feeds the first half of its bytes to a digest, then fails. */
  private static final class FailingHalfwayBytes extends AbstractBytes {
    private final Bytes bytes;

    private FailingHalfwayBytes(final Bytes bytes) {
      this.bytes = bytes;
    }

    @Override
    public void update(final MessageDigest digest) {
      bytes.slice(0, bytes.size() / 2).update(digest);
      throw new IllegalStateException("failed halfway");
    }

    @Override
    public int size() {
      return bytes.size();
    }

    @Override
    public byte get(final int i) {
      return bytes.get(i);
    }

    @Override
    public Bytes slice(final int i, final int length) {
      return bytes.slice(i, length);
    }

    @Override
    public Bytes copy() {
      return bytes.copy();
    }

    @Override
    public MutableBytes mutableCopy() {
      return bytes.mutableCopy();
    }

    @Override
    public String toString() {
      return bytes.toString();
    }
  }
}
