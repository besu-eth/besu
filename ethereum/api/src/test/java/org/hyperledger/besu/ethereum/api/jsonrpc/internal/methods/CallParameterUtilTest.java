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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CallParameterUtilTest {

  @ParameterizedTest
  @MethodSource("mixedFeeFields")
  void rejectsGasPriceWithDynamicFees(final UnaryOperator<ImmutableCallParameter.Builder> fees) {
    assertThatThrownBy(
            () ->
                CallParameterUtil.rejectMixedFeeFields(
                    fees.apply(ImmutableCallParameter.builder().gasPrice(Wei.ONE)).build()))
        .isInstanceOfSatisfying(
            InvalidJsonRpcParameters.class,
            e -> assertThat(e.getRpcErrorType()).isEqualTo(RpcErrorType.INVALID_PARAMS));
  }

  static Stream<UnaryOperator<ImmutableCallParameter.Builder>> mixedFeeFields() {
    return Stream.of(
        params -> params.maxFeePerGas(Wei.ONE),
        params -> params.maxPriorityFeePerGas(Wei.ZERO),
        params -> params.maxFeePerGas(Wei.ONE).maxPriorityFeePerGas(Wei.ONE));
  }

  @Test
  void acceptsGasPriceWithBlobFields() {
    assertThatNoException()
        .isThrownBy(
            () ->
                CallParameterUtil.rejectMixedFeeFields(
                    ImmutableCallParameter.builder()
                        .gasPrice(Wei.ONE)
                        .maxFeePerBlobGas(Wei.ONE)
                        .blobVersionedHashes(List.of(VersionedHash.DEFAULT_VERSIONED_HASH))
                        .build()));
  }

  @Test
  void allowsExceedingBalanceWhenAll1559FeesAreZero() {
    assertThat(isAllowExceedingBalance(params -> params)).isTrue();
  }

  @Test
  void checksBalanceWhenMaxFeePerGasIsNonZero() {
    assertThat(isAllowExceedingBalance(params -> params.maxFeePerGas(Wei.ONE))).isFalse();
  }

  @Test
  void checksBalanceWhenMaxPriorityFeePerGasIsNonZero() {
    assertThat(isAllowExceedingBalance(params -> params.maxPriorityFeePerGas(Wei.ONE))).isFalse();
  }

  @ParameterizedTest
  @MethodSource("blobFeeCaps")
  void allowsExceedingBalanceWhenBlobTxHasNoExecutionFees(final Optional<Wei> maxFeePerBlobGas) {
    assertThat(
            isAllowExceedingBalance(
                params ->
                    params
                        .blobVersionedHashes(List.of(VersionedHash.DEFAULT_VERSIONED_HASH))
                        .maxFeePerBlobGas(maxFeePerBlobGas)))
        .isTrue();
  }

  // the blob fee is priced on its own and doesn't make the execution fees unpriced
  @ParameterizedTest
  @MethodSource("blobFeeCaps")
  void checksBalanceWhenBlobTxHasExecutionFees(final Optional<Wei> maxFeePerBlobGas) {
    assertThat(
            isAllowExceedingBalance(
                params ->
                    params
                        .blobVersionedHashes(List.of(VersionedHash.DEFAULT_VERSIONED_HASH))
                        .maxFeePerBlobGas(maxFeePerBlobGas)
                        .maxFeePerGas(Wei.ONE)))
        .isFalse();
  }

  static Stream<Optional<Wei>> blobFeeCaps() {
    return Stream.of(Optional.empty(), Optional.of(Wei.ZERO), Optional.of(Wei.ONE));
  }

  @Test
  void strictFlagWinsOverZeroFees() {
    assertThat(isAllowExceedingBalance(params -> params.strict(true))).isFalse();
  }

  private boolean isAllowExceedingBalance(
      final UnaryOperator<ImmutableCallParameter.Builder> customise) {
    final BlockHeader header = mock(BlockHeader.class);
    when(header.getBaseFee()).thenReturn(Optional.of(Wei.of(11)));
    return CallParameterUtil.isAllowExceedingBalance(
        header, customise.apply(ImmutableCallParameter.builder()).build());
  }
}
