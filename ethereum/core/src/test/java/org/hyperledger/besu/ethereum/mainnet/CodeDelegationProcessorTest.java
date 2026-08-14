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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.CodeDelegationService;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
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
  private static final Address TX_SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address TX_TO =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address AUTHORITY =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address OTHER_DELEGATE_ADDRESS =
      Address.fromHexString("0x4444444444444444444444444444444444444444");

  @BeforeEach
  void setUp() {
    processor =
        new CodeDelegationProcessor(Optional.of(CHAIN_ID), HALF_CURVE_ORDER, codeDelegationService);
    lenient().when(transaction.getSender()).thenReturn(TX_SENDER);
    lenient().when(transaction.getValue()).thenReturn(Wei.ZERO);
    lenient().when(transaction.getTo()).thenReturn(Optional.of(TX_TO));
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

  @Test
  void shouldNotRefundAuthBaseForNewAccountWithNonZeroDelegateAddress() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 0L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(null);
    when(worldUpdater.createAccount(any())).thenReturn(authority);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.authBaseRefundCount()).isZero();
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(authority).incrementNonce();
  }

  @Test
  void shouldRefundAuthBaseForNewAccountClearingDelegation() {
    // Arrange
    CodeDelegation codeDelegation =
        createCodeDelegation(CHAIN_ID, 0L, BigInteger.ONE, Address.ZERO);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(null);
    when(worldUpdater.createAccount(any())).thenReturn(authority);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.authBaseRefundCount()).isEqualTo(1);
    assertThat(result.alreadyExistingDelegators()).isZero();
    verify(authority).incrementNonce();
  }

  @Test
  void shouldRefundAuthBaseForExistingAccountWithExistingDelegation() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 1L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(authority);
    when(worldUpdater.getAccount(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(1L);
    when(authority.getCode())
        .thenReturn(
            Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, Bytes.random(20)));
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.authBaseRefundCount()).isEqualTo(1);
    assertThat(result.alreadyExistingDelegators()).isEqualTo(1);
    verify(authority).incrementNonce();
  }

  @Test
  void shouldNotRefundAuthBaseForExistingAccountWithoutDelegation() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 1L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(authority);
    when(worldUpdater.getAccount(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(1L);
    when(authority.getCode()).thenReturn(Bytes.EMPTY);
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.authBaseRefundCount()).isZero();
    assertThat(result.alreadyExistingDelegators()).isEqualTo(1);
    verify(authority).incrementNonce();
  }

  @Test
  void shouldRefundAuthBaseOnceWhenSameAuthorityIsDelegatedTwiceInOneTransaction() {
    // Arrange: two authorizations for one authority. The first writes fresh indicator bytes, the
    // second overwrites them in place.
    CodeDelegation first = delegationForAuthority(DELEGATE_ADDRESS);
    CodeDelegation second = delegationForAuthority(OTHER_DELEGATE_ADDRESS);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(first, second)));
    // Absent for the first authorization, then present carrying the designator it wrote.
    when(worldUpdater.get(any())).thenReturn(null).thenReturn(authority);
    when(worldUpdater.createAccount(any())).thenReturn(authority);
    when(worldUpdater.getAccount(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(0L);
    when(authority.getCode()).thenReturn(delegationCode());
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert: only the second authorization overwrote an existing designator.
    assertThat(result.authBaseRefundCount()).isEqualTo(1);
    assertThat(result.alreadyExistingDelegators()).isEqualTo(1);
    assertThat(result.invalidAuthorizations()).isZero();
    verify(authority, times(2)).incrementNonce();
  }

  @Test
  void shouldRefundAuthBaseTwiceWhenDelegationWrittenInTransactionIsThenCleared() {
    // Arrange: one authority delegated and then cleared within the same transaction.
    CodeDelegation set = delegationForAuthority(DELEGATE_ADDRESS);
    CodeDelegation clear = delegationForAuthority(Address.ZERO);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(set, clear)));
    when(worldUpdater.get(any())).thenReturn(null).thenReturn(authority);
    when(worldUpdater.createAccount(any())).thenReturn(authority);
    when(worldUpdater.getAccount(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(0L);
    when(authority.getCode()).thenReturn(delegationCode());
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert: clearing writes no indicator bytes, and the designator it cleared was itself paid
    // for earlier in this transaction, so both authorizations refund AUTH_BASE.
    assertThat(result.authBaseRefundCount()).isEqualTo(2);
    verify(authority, times(2)).incrementNonce();
  }

  @Test
  void shouldRefundAuthBaseOnceWhenClearingDelegationThatPredatesTheTransaction() {
    // Arrange: the designator being cleared was not written by this transaction, so unlike the
    // case above there is no second AUTH_BASE to give back.
    CodeDelegation clear = delegationForAuthority(Address.ZERO);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(clear)));
    when(worldUpdater.get(any())).thenReturn(authority);
    when(worldUpdater.getAccount(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(0L);
    when(authority.getCode()).thenReturn(delegationCode());
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.authBaseRefundCount()).isEqualTo(1);
    assertThat(result.alreadyExistingDelegators()).isEqualTo(1);
    verify(authority).incrementNonce();
  }

  private static Bytes delegationCode() {
    return Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, Bytes.random(20));
  }

  /**
   * A nonce-0 authorization for {@link #AUTHORITY}. Mocked rather than signed, so that the delegate
   * address can vary while the recovered authority stays the same.
   */
  private CodeDelegation delegationForAuthority(final Address delegateAddress) {
    final CodeDelegation delegation = mock(CodeDelegation.class);
    when(delegation.chainId()).thenReturn(CHAIN_ID);
    when(delegation.nonce()).thenReturn(0L);
    when(delegation.address()).thenReturn(delegateAddress);
    when(delegation.signature())
        .thenReturn(new SECPSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0));
    when(delegation.authorizer()).thenReturn(Optional.of(AUTHORITY));
    return delegation;
  }

  @Test
  void shouldTreatPresentButEmptyAccountAsExistingDelegator() {
    // Arrange: an account that exists in the updater but is empty (nonce 0, balance 0, no code).
    // No new leaf is added for it, so it counts as an already-existing delegator and the
    // worst-case NEW_ACCOUNT / ACCOUNT_WRITE reserved at the intrinsic phase is refunded.
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 0L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(authority);
    when(worldUpdater.getAccount(any())).thenReturn(authority);
    when(authority.getNonce()).thenReturn(0L);
    when(authority.getCode()).thenReturn(Bytes.EMPTY);
    when(codeDelegationService.canSetCodeDelegation(any())).thenReturn(true);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.alreadyExistingDelegators()).isEqualTo(1);
    // A fresh (non-zero) delegation designator is written, so AUTH_BASE is not refunded.
    assertThat(result.authBaseRefundCount()).isZero();
    verify(worldUpdater, never()).createAccount(any());
    verify(authority).incrementNonce();
  }

  @Test
  void shouldCountInvalidAuthorizationForRejectedAuthorization() {
    // Arrange: nonce mismatch against a non-existent authority — the authorization is rejected.
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 1L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(null);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert: an invalid authorization grows no state, so its full worst-case intrinsic charge
    // is refunded.
    assertThat(result.invalidAuthorizations()).isEqualTo(1);
    verify(worldUpdater, never()).createAccount(any());
  }

  @Test
  void shouldNotCountInvalidAuthorizationForAcceptedAuthorization() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(CHAIN_ID, 0L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));
    when(worldUpdater.get(any())).thenReturn(null);
    when(worldUpdater.createAccount(any())).thenReturn(authority);

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.invalidAuthorizations()).isZero();
    verify(authority).incrementNonce();
  }

  @Test
  void shouldCountInvalidAuthorizationForWrongChainId() {
    // Arrange
    CodeDelegation codeDelegation = createCodeDelegation(BigInteger.valueOf(999), 0L);
    when(transaction.getCodeDelegationList()).thenReturn(Optional.of(List.of(codeDelegation)));

    // Act
    CodeDelegationResult result = processor.process(worldUpdater, transaction, Optional.empty());

    // Assert
    assertThat(result.invalidAuthorizations()).isEqualTo(1);
    verify(worldUpdater, never()).createAccount(any());
  }

  private CodeDelegation createCodeDelegation(final BigInteger chainId, final long nonce) {
    return createCodeDelegation(chainId, nonce, BigInteger.ONE);
  }

  private CodeDelegation createCodeDelegation(
      final BigInteger chainId, final long nonce, final BigInteger s) {
    return createCodeDelegation(chainId, nonce, s, CodeDelegationProcessorTest.DELEGATE_ADDRESS);
  }

  private CodeDelegation createCodeDelegation(
      final BigInteger chainId, final long nonce, final BigInteger s, final Address address) {
    final SECPSignature signature = new SECPSignature(BigInteger.ONE, s, (byte) 0);

    return new org.hyperledger.besu.ethereum.core.CodeDelegation(
        chainId, address, nonce, signature);
  }
}
