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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.CodeDelegationService;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CodeDelegationProcessorTest {

  @Mock private WorldUpdater worldUpdater;

  @Mock private Transaction transaction;

  @Mock private MutableAccount authority;

  @Mock private CodeDelegationService codeDelegationService;

  private CodeDelegationProcessor processor;
  private static final BigInteger CHAIN_ID = BigInteger.valueOf(1);
  private static final BigInteger HALF_CURVE_ORDER = BigInteger.valueOf(1000);
  private static final Address DELEGATE_ADDRESS =
      Address.fromHexString("0x9876543210987654321098765432109876543210");

  @BeforeEach
  void setUp() {
    processor =
        new CodeDelegationProcessor(Optional.of(CHAIN_ID), HALF_CURVE_ORDER, codeDelegationService);
  }

  @Test
  void shouldRejectInvalidChainId() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(BigInteger.valueOf(2), 0L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(worldUpdater, never()).createAccount(any());
    verify(worldUpdater, never()).getAccount(any());
  }

  @Test
  void shouldRejectMaxNonce() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, Account.MAX_NONCE);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(worldUpdater, never()).createAccount(any());
    verify(worldUpdater, never()).getAccount(any());
  }

  @Test
  void shouldProcessValidDelegationForNewAccount() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 0L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(null);
    when(worldUpdater.createAccount(any())).thenReturn(authority);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(worldUpdater).createAccount(any());
    verify(authority).incrementNonce();
  }

  @Test
  void shouldNotCreateAccountIfNonceIsInvalid() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 1L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(null);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(worldUpdater, never()).createAccount(any());
    verify(authority, never()).incrementNonce();
  }

  @Test
  void shouldProcessValidDelegationForExistingAccount() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 1L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(authority);
    when(worldUpdater.getAccount(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(1L);
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isEqualTo(1);
    verify(worldUpdater, never()).createAccount(any());
    verify(authority).incrementNonce();
    verify(codeDelegationService).processCodeDelegation(authority, DELEGATE_ADDRESS);
  }

  @Test
  void shouldRejectDelegationWithInvalidNonce() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 2L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(1L);
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(worldUpdater, never()).getAccount(any());
    verify(authority, never()).incrementNonce();
    verify(codeDelegationService, never()).processCodeDelegation(any(), any());
  }

  @Test
  void shouldSkipOverInvalidMultipleInvalidNonceDelegationsForSameAuthorityForNewAccount() {
    // Arrange
    var signature1 = new SECPSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0);
    long cd1_invalidNonce = 2L;
    var cd1_invalid =
        new org.hyperledger.besu.ethereum.core.CodeDelegation(
            CHAIN_ID,
            Address.fromHexString("0x0000000000000000000000000000000000001000"),
            cd1_invalidNonce,
            signature1);
    var signature2 = new SECPSignature(BigInteger.TWO, BigInteger.TWO, (byte) 0);
    final long cd2_validNonce = 0L;
    var cd2_valid =
        new org.hyperledger.besu.ethereum.core.CodeDelegation(
            CHAIN_ID,
            Address.fromHexString("0x0000000000000000000000000000000000001100"),
            cd2_validNonce,
            signature2);
    var signature3 = new SECPSignature(BigInteger.TWO, BigInteger.TWO, (byte) 0);
    final long cd3_invalidNonce = 0L;
    var cd3_invalid =
        new org.hyperledger.besu.ethereum.core.CodeDelegation(
            CHAIN_ID,
            Address.fromHexString("0x0000000000000000000000000000000000001200"),
            cd3_invalidNonce,
            signature3);
    when(transaction.getCodeDelegationList())
        .thenReturn(Optional.of(List.of(cd1_invalid, cd2_valid, cd3_invalid)));

    when(worldUpdater.get(any())).thenReturn(null).thenReturn(null).thenReturn(authority);
    when(worldUpdater.createAccount(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(1L);
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(authority, times(1)).incrementNonce();
    verify(codeDelegationService, times(1)).processCodeDelegation(any(), any());
  }

  @Test
  void shouldRejectDelegationWithSGreaterThanHalfCurveOrder() {
    // Arrange
    CodeDelegation codeDelegation =
        createCodeDelegation(CHAIN_ID, 1L, HALF_CURVE_ORDER.add(BigInteger.ONE));
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(authority, never()).incrementNonce();
    verify(codeDelegationService, never()).processCodeDelegation(any(), any());
  }

  @Test
  void shouldRejectDelegationWithRecIdNeitherZeroNorOne() {
    // Arrange
    final SECPSignature signature = new SECPSignature(BigInteger.ONE, BigInteger.ONE, (byte) 2);
    CodeDelegation codeDelegation =
        new org.hyperledger.besu.ethereum.core.CodeDelegation(
            CHAIN_ID, CodeDelegationProcessorTest.DELEGATE_ADDRESS, 1L, signature);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(authority, never()).incrementNonce();
    verify(codeDelegationService, never()).processCodeDelegation(any(), any());
  }

  @Test
  void shouldRejectDelegationWithInvalidSignature() {
    // Arrange
    CodeDelegation codeDelegation = mock(org.hyperledger.besu.ethereum.core.CodeDelegation.class);
    when(codeDelegation.chainId()).thenReturn(CHAIN_ID);
    when(codeDelegation.nonce()).thenReturn(1L);
    when(codeDelegation.signature())
        .thenReturn(new SECPSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0));
    when(codeDelegation.authorizer()).thenReturn(Optional.empty());
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(authority, never()).incrementNonce();
    verify(codeDelegationService, never()).processCodeDelegation(any(), any());
  }

  @Test
  void shouldRejectDelegationWhenCannotSetCodeDelegation() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 1L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(authority);
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(false);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(worldUpdater, never()).getAccount(any());
    verify(authority, never()).incrementNonce();
    verify(codeDelegationService, never()).processCodeDelegation(any(), any());
  }

  private CodeDelegation createCodeDelegation(final BigInteger chainId, final long nonce) {
    return createCodeDelegation(chainId, nonce, BigInteger.ONE);
  }

  private CodeDelegation createCodeDelegation(
      final BigInteger chainId, final long nonce, final BigInteger s) {
    final SECPSignature signature = new SECPSignature(BigInteger.ONE, s, (byte) 0);

    return new org.hyperledger.besu.ethereum.core.CodeDelegation(
        chainId, CodeDelegationProcessorTest.DELEGATE_ADDRESS, nonce, signature);
  }
}
