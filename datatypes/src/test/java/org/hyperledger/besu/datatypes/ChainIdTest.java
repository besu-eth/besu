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
package org.hyperledger.besu.datatypes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ChainIdTest {
  private static final BigInteger TWO_POW_63 = BigInteger.ONE.shiftLeft(63);
  private static final BigInteger TWO_POW_256_MINUS_1 =
      BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);

  @AfterEach
  void resetNetworkChainId() {
    ChainId.resetNetworkChainId();
  }

  static Stream<BigInteger> values() {
    return Stream.of(
        BigInteger.ZERO,
        BigInteger.ONE,
        BigInteger.valueOf(127),
        BigInteger.valueOf(128),
        BigInteger.valueOf(560048),
        BigInteger.ONE.shiftLeft(56).subtract(BigInteger.ONE),
        BigInteger.ONE.shiftLeft(56),
        BigInteger.valueOf(Long.MAX_VALUE),
        TWO_POW_63,
        TWO_POW_256_MINUS_1);
  }

  @ParameterizedTest
  @MethodSource("values")
  void factoriesAgreeOnTheValue(final BigInteger value) {
    final ChainId chainId = ChainId.of(value);

    assertThat(chainId.isPresent()).isTrue();
    assertThat(chainId.isAbsent()).isFalse();
    assertThat(chainId.toBigInteger()).isEqualTo(value);
    assertThat(chainId.toOptionalBigInteger()).contains(value);
    assertThat(chainId.fitsLong()).isEqualTo(value.bitLength() < Long.SIZE);
    assertThat(chainId.toString()).isEqualTo(value.toString());
    assertThat(chainId.toHexString()).isEqualTo("0x" + value.toString(16));

    assertThat(ChainId.fromBytes(Bytes.wrap(value.toByteArray()))).isEqualTo(chainId);
    assertThat(ChainId.fromString(value.toString())).isEqualTo(chainId);
    assertThat(ChainId.fromString(chainId.toHexString())).isEqualTo(chainId);
    assertThat(ChainId.fromOptional(Optional.of(value))).isEqualTo(chainId);
    if (chainId.fitsLong()) {
      assertThat(ChainId.of(value.longValueExact())).isEqualTo(chainId);
      assertThat(chainId.toLong()).isEqualTo(value.longValueExact());
    } else {
      assertThatThrownBy(chainId::toLong).isInstanceOf(ArithmeticException.class);
    }
    assertThat(chainId.hashCode()).isEqualTo(ChainId.of(value).hashCode());
  }

  @ParameterizedTest
  @MethodSource("values")
  void rlpEncodingIsTheSameAsTheBigIntegerScalarOne(final BigInteger value) {
    final ChainId chainId = ChainId.of(value);

    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    chainId.writeTo(out);
    final BytesValueRLPOutput expected = new BytesValueRLPOutput();
    expected.writeBigIntegerScalar(value);
    assertThat(out.encoded()).isEqualTo(expected.encoded());

    assertThat(ChainId.readFrom(new BytesValueRLPInput(out.encoded(), false))).isEqualTo(chainId);
    assertThat(ChainId.readFrom(new BytesValueRLPInput(out.encoded(), false), 32))
        .isEqualTo(chainId);
  }

  @Test
  void readFromRejectsValuesLargerThanTheMaxSize() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeBigIntegerScalar(TWO_POW_63);

    assertThatThrownBy(() -> ChainId.readFrom(new BytesValueRLPInput(out.encoded(), false), 7))
        .isInstanceOf(RLPException.class);
  }

  @Test
  void readFromRejectsLeadingZeros() {
    final Bytes encodedWithLeadingZero = Bytes.fromHexString("0x820001");

    assertThatThrownBy(
            () -> ChainId.readFrom(new BytesValueRLPInput(encodedWithLeadingZero, false)))
        .isInstanceOf(RLPException.class);
  }

  @Test
  void valuesThatFitInLongAreNeverBackedByBigInteger() {
    final ChainId fromBytes = ChainId.fromBytes(Bytes.fromHexString("0x00000000000000000005"));

    assertThat(fromBytes).isEqualTo(ChainId.of(5)).isEqualTo(ChainId.of(BigInteger.valueOf(5)));
    assertThat(fromBytes.fitsLong()).isTrue();
    assertThat(ChainId.fromBytes(Bytes.EMPTY)).isEqualTo(ChainId.of(0));
  }

  @Test
  void zero() {
    final ChainId zero = ChainId.of(0);

    assertThat(zero.isZero()).isTrue();
    assertThat(zero.isPresent()).isTrue();
    assertThat(ChainId.of(1).isZero()).isFalse();
    assertThat(ChainId.of(BigInteger.ZERO)).isSameAs(zero);
    assertThat(ChainId.fromBytes(Bytes.of(0))).isSameAs(zero);
  }

  @Test
  void absent() {
    final ChainId absent = ChainId.absent();

    assertThat(absent.isAbsent()).isTrue();
    assertThat(absent.isPresent()).isFalse();
    assertThat(absent.isZero()).isFalse();
    assertThat(absent.fitsLong()).isFalse();
    assertThat(absent.toOptionalBigInteger()).isEmpty();
    assertThat(absent).isEqualTo(ChainId.fromOptional(Optional.empty()));
    assertThat(absent).isNotEqualTo(ChainId.of(0));
    assertThat(absent.toString()).isEqualTo("absent");
    assertThatThrownBy(absent::toBigInteger).isInstanceOf(NoSuchElementException.class);
    assertThatThrownBy(absent::toLong).isInstanceOf(NoSuchElementException.class);
    assertThatThrownBy(absent::toHexString).isInstanceOf(NoSuchElementException.class);
    assertThatThrownBy(() -> absent.writeTo(new BytesValueRLPOutput()))
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void negativeValuesAreRejected() {
    assertThatThrownBy(() -> ChainId.of(-1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ChainId.of(BigInteger.valueOf(-1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ChainId.fromString("-1")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void differentValuesAreNotEqual() {
    assertThat(ChainId.of(1)).isNotEqualTo(ChainId.of(2));
    assertThat(ChainId.of(TWO_POW_63)).isNotEqualTo(ChainId.of(TWO_POW_256_MINUS_1));
    assertThat(ChainId.of(Long.MAX_VALUE)).isNotEqualTo(ChainId.of(TWO_POW_63));
  }

  @Test
  void networkChainIdIsSharedByAllTheFactories() {
    final ChainId network = ChainId.initNetworkChainId(ChainId.of(560048));
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    network.writeTo(out);

    assertThat(ChainId.of(560048)).isSameAs(network);
    assertThat(ChainId.of(BigInteger.valueOf(560048))).isSameAs(network);
    assertThat(ChainId.fromBytes(Bytes.ofUnsignedInt(560048))).isSameAs(network);
    assertThat(ChainId.fromString("560048")).isSameAs(network);
    assertThat(ChainId.fromString("0x88bb0")).isSameAs(network);
    assertThat(ChainId.readFrom(new BytesValueRLPInput(out.encoded(), false))).isSameAs(network);
    assertThat(ChainId.of(560049)).isNotSameAs(network).isNotEqualTo(network);
  }

  @Test
  void networkChainIdConversionsAreCached() {
    final ChainId network = ChainId.initNetworkChainId(ChainId.of(560048));

    assertThat(network.toBigInteger()).isSameAs(network.toBigInteger());
    assertThat(network.toOptionalBigInteger()).isSameAs(network.toOptionalBigInteger());
    assertThat(ChainId.of(560048).toOptionalBigInteger()).isSameAs(network.toOptionalBigInteger());
  }

  @Test
  void networkChainIdLargerThanLongIsShared() {
    final ChainId network = ChainId.initNetworkChainId(ChainId.of(TWO_POW_256_MINUS_1));

    assertThat(ChainId.of(TWO_POW_256_MINUS_1)).isSameAs(network);
    assertThat(ChainId.fromBytes(Bytes.wrap(TWO_POW_256_MINUS_1.toByteArray()))).isSameAs(network);
    assertThat(network.toOptionalBigInteger()).isSameAs(network.toOptionalBigInteger());
  }

  @Test
  void networkChainIdCanBeInitializedOnlyOnce() {
    ChainId.initNetworkChainId(ChainId.of(560048));

    assertThatThrownBy(() -> ChainId.initNetworkChainId(ChainId.of(560048)))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> ChainId.initNetworkChainId(ChainId.of(1)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void absentNetworkChainIdCanBeInitializedOnlyOnce() {
    ChainId.initNetworkChainId(ChainId.absent());

    assertThat(ChainId.of(1)).isNotSameAs(ChainId.of(1));
    assertThatThrownBy(() -> ChainId.initNetworkChainId(ChainId.of(1)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void resettingTheNetworkChainIdStopsSharing() {
    final ChainId network = ChainId.initNetworkChainId(ChainId.of(560048));
    ChainId.resetNetworkChainId();

    assertThat(ChainId.of(560048)).isNotSameAs(network).isEqualTo(network);
    assertThat(ChainId.initNetworkChainId(ChainId.of(1))).isSameAs(ChainId.of(1));
  }
}
