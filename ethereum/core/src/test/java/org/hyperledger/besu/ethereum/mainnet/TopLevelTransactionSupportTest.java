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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.Eip7928AccessList;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.LondonGasCalculator;
import org.hyperledger.besu.evm.internal.CodeCache;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class TopLevelTransactionSupportTest {

  private static final Address RECIPIENT =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address TARGET =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address COINBASE =
      Address.fromHexString("0x4242424242424242424242424242424242424242");
  private static final Address OTHER =
      Address.fromHexString("0x3333333333333333333333333333333333333333");

  private static final Bytes CONTRACT_CODE = Bytes.fromHexString("0x6001600055");
  private static final Bytes DELEGATION_CODE =
      Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, TARGET.getBytes());

  private static final Wei BASE_FEE = Wei.of(5);

  private final GasCalculator london = new LondonGasCalculator();
  private final GasCalculator amsterdam = new AmsterdamGasCalculator();
  private final MessageCallProcessor messageCallProcessor = mock(MessageCallProcessor.class);
  private final CoinbaseFeePriceCalculator configuredCalculator =
      (coinbaseFee, gasPrice, baseFee) -> Wei.of(42);

  private TopLevelTransactionSupport support(final GasCalculator gasCalculator) {
    return new TopLevelTransactionSupport(
        gasCalculator, messageCallProcessor, configuredCalculator);
  }

  // accountCode

  @Test
  void nullAccountRunsEmptyCode() {
    assertThat(support(london).accountCode(null, TopLevelTransactionSupportTest::noDelegation))
        .isSameAs(Code.EMPTY_CODE);
    verifyNoInteractions(messageCallProcessor);
  }

  @Test
  void accountWithoutCodeHashRunsEmptyCode() {
    final Account account = mock(Account.class);
    when(account.getCodeHash()).thenReturn(null);

    assertThat(support(london).accountCode(account, TopLevelTransactionSupportTest::noDelegation))
        .isSameAs(Code.EMPTY_CODE);
    verifyNoInteractions(messageCallProcessor);
  }

  @Test
  void accountWithEmptyCodeHashRunsEmptyCode() {
    final Account account = mock(Account.class);
    when(account.getCodeHash()).thenReturn(Hash.EMPTY);

    assertThat(support(london).accountCode(account, TopLevelTransactionSupportTest::noDelegation))
        .isSameAs(Code.EMPTY_CODE);
    verifyNoInteractions(messageCallProcessor);
  }

  @Test
  void delegatedAccountResolvesThroughTheCallersFunction() {
    final Account account = mock(Account.class);
    when(account.getCodeHash()).thenReturn(Hash.hash(DELEGATION_CODE));
    when(account.getCode()).thenReturn(DELEGATION_CODE);
    final Code targetCode = new Code(CONTRACT_CODE);

    final Code code =
        support(london)
            .accountCode(
                account,
                delegated -> {
                  assertThat(delegated).isSameAs(account);
                  return targetCode;
                });

    assertThat(code).isSameAs(targetCode);
    verifyNoInteractions(messageCallProcessor);
  }

  @Test
  void accountWithCodeCacheRunsItsCachedCode() {
    final Account account = mock(Account.class);
    final Code cachedCode = new Code(CONTRACT_CODE);
    when(account.getCodeHash()).thenReturn(Hash.hash(CONTRACT_CODE));
    when(account.getCode()).thenReturn(CONTRACT_CODE);
    when(account.getCodeCache()).thenReturn(mock(CodeCache.class));
    when(account.getOrCreateCachedCode()).thenReturn(cachedCode);

    assertThat(support(london).accountCode(account, TopLevelTransactionSupportTest::noDelegation))
        .isSameAs(cachedCode);
    verifyNoInteractions(messageCallProcessor);
  }

  @Test
  void accountWithoutCodeCacheRunsTheJumpDestCachedCode() {
    final Account account = mock(Account.class);
    final Hash codeHash = Hash.hash(CONTRACT_CODE);
    final Code analysedCode = new Code(CONTRACT_CODE);
    when(account.getCodeHash()).thenReturn(codeHash);
    when(account.getCode()).thenReturn(CONTRACT_CODE);
    when(messageCallProcessor.getOrCreateCachedJumpDest(codeHash, CONTRACT_CODE))
        .thenReturn(analysedCode);

    assertThat(support(london).accountCode(account, TopLevelTransactionSupportTest::noDelegation))
        .isSameAs(analysedCode);
  }

  // coinbaseCalculator

  @Test
  void noBaseFeeUsesTheFrontierCalculator() {
    final Optional<CoinbaseFeePriceCalculator> calculator =
        support(london)
            .coinbaseCalculator(
                header(Optional.empty()), Wei.of(7), TransactionValidationParams.processingBlock());

    assertThat(calculator).isPresent();
    assertThat(calculator.get().price(10L, Wei.of(7), Optional.empty())).isEqualTo(Wei.of(70));
  }

  @Test
  void priceAtOrAboveBaseFeeUsesTheConfiguredCalculator() {
    final TopLevelTransactionSupport support = support(london);
    final ProcessableBlockHeader header = header(Optional.of(BASE_FEE));

    assertThat(
            support.coinbaseCalculator(
                header, BASE_FEE, TransactionValidationParams.processingBlock()))
        .containsSame(configuredCalculator);
    assertThat(
            support.coinbaseCalculator(
                header, Wei.of(6), TransactionValidationParams.processingBlock()))
        .containsSame(configuredCalculator);
  }

  @Test
  void priceBelowBaseFeeIsRejected() {
    assertThat(
            support(london)
                .coinbaseCalculator(
                    header(Optional.of(BASE_FEE)),
                    Wei.of(4),
                    TransactionValidationParams.processingBlock()))
        .isEmpty();
  }

  @Test
  void priceBelowBaseFeePaysNothingWhenUnderpricedGasIsAllowed() {
    final Optional<CoinbaseFeePriceCalculator> calculator =
        support(london)
            .coinbaseCalculator(
                header(Optional.of(BASE_FEE)),
                Wei.of(4),
                ImmutableTransactionValidationParams.builder().allowUnderpricedGas(true).build());

    assertThat(calculator).isPresent();
    assertThat(calculator.get()).isNotSameAs(configuredCalculator);
    assertThat(calculator.get().price(10L, Wei.of(4), Optional.of(BASE_FEE))).isEqualTo(Wei.ZERO);
  }

  @Test
  void priceBelowBaseFeePaysNothingWhenCallerGasPricingIsPreserved() {
    final Optional<CoinbaseFeePriceCalculator> calculator =
        support(london)
            .coinbaseCalculator(
                header(Optional.of(BASE_FEE)),
                Wei.of(4),
                ImmutableTransactionValidationParams.builder()
                    .isPreserveCallerGasPricing(true)
                    .build());

    assertThat(calculator).isPresent();
    assertThat(calculator.get().price(10L, Wei.of(4), Optional.of(BASE_FEE))).isEqualTo(Wei.ZERO);
  }

  @Test
  void underpricingAllowanceKeepsTheConfiguredCalculatorAtOrAboveBaseFee() {
    assertThat(
            support(london)
                .coinbaseCalculator(
                    header(Optional.of(BASE_FEE)),
                    BASE_FEE,
                    ImmutableTransactionValidationParams.builder()
                        .allowUnderpricedGas(true)
                        .build()))
        .containsSame(configuredCalculator);
  }

  // creditCoinbase

  @Test
  void zeroFeeStillTouchesTheCoinbase() {
    final WorldUpdater worldUpdater = mock(WorldUpdater.class);
    final MutableAccount coinbase = mock(MutableAccount.class);
    final AccessLocationTracker tracker = mock(AccessLocationTracker.class);
    when(worldUpdater.getOrCreate(COINBASE)).thenReturn(coinbase);

    TopLevelTransactionSupport.creditCoinbase(
        worldUpdater, COINBASE, Wei.ZERO, Optional.of(tracker));

    verify(worldUpdater).getOrCreate(COINBASE);
    verify(tracker).addTouchedAccount(COINBASE);
    verify(coinbase, never()).incrementBalance(any());
  }

  @Test
  void positiveFeeIsCreditedToTheCoinbase() {
    final WorldUpdater worldUpdater = mock(WorldUpdater.class);
    final MutableAccount coinbase = mock(MutableAccount.class);
    when(worldUpdater.getOrCreate(COINBASE)).thenReturn(coinbase);

    TopLevelTransactionSupport.creditCoinbase(worldUpdater, COINBASE, Wei.of(21), Optional.empty());

    verify(coinbase).incrementBalance(Wei.of(21));
  }

  // chargeAccountAccess

  @Test
  void coldAccessIsChargedTheColdCost() {
    final MessageFrame frame = frameWithGas(10_000L);
    when(frame.warmUpAddress(TARGET)).thenReturn(false);

    assertThat(support(london).chargeAccountAccess(frame, TARGET)).isTrue();
    verify(frame).decrementRemainingGas(london.getColdAccountAccessCost());
  }

  @Test
  void warmAccessIsChargedTheWarmCost() {
    final MessageFrame frame = frameWithGas(10_000L);
    when(frame.warmUpAddress(TARGET)).thenReturn(true);

    assertThat(support(london).chargeAccountAccess(frame, TARGET)).isTrue();
    verify(frame).decrementRemainingGas(london.getWarmStorageReadCost());
  }

  @Test
  void precompileIsChargedTheWarmCostAndStillWarmed() {
    final MessageFrame frame = frameWithGas(10_000L);
    when(frame.warmUpAddress(Address.ECREC)).thenReturn(false);

    assertThat(support(london).chargeAccountAccess(frame, Address.ECREC)).isTrue();
    verify(frame).warmUpAddress(Address.ECREC);
    verify(frame).decrementRemainingGas(london.getWarmStorageReadCost());
  }

  @Test
  void unaffordableAccessChargesNothing() {
    final MessageFrame frame = frameWithGas(london.getColdAccountAccessCost() - 1);
    when(frame.warmUpAddress(TARGET)).thenReturn(false);

    assertThat(support(london).chargeAccountAccess(frame, TARGET)).isFalse();
    verify(frame, never()).decrementRemainingGas(anyLong());
  }

  // chargeTransactionEntry

  @Test
  void valueToAnEmptyRecipientConsumesNewAccountStateGas() {
    final long newAccountStateGas = amsterdam.stateGasCostCalculator().newAccountStateGas();
    final MessageFrame frame = frameWithGas(10_000L);
    final WorldUpdater worldState = mock(WorldUpdater.class);
    when(frame.getValue()).thenReturn(Wei.ONE);
    when(frame.consumeStateGas(newAccountStateGas)).thenReturn(true);

    assertThat(support(amsterdam).chargeTransactionEntry(frame, worldState, RECIPIENT)).isTrue();
    verify(frame).consumeStateGas(newAccountStateGas);
  }

  @Test
  void unaffordableNewAccountStateGasFailsTheEntry() {
    final long newAccountStateGas = amsterdam.stateGasCostCalculator().newAccountStateGas();
    final MessageFrame frame = frameWithGas(10_000L);
    final WorldUpdater worldState = mock(WorldUpdater.class);
    final Account emptyRecipient = mock(Account.class);
    when(worldState.get(RECIPIENT)).thenReturn(emptyRecipient);
    when(emptyRecipient.isEmpty()).thenReturn(true);
    when(frame.getValue()).thenReturn(Wei.ONE);
    when(frame.consumeStateGas(newAccountStateGas)).thenReturn(false);

    assertThat(support(amsterdam).chargeTransactionEntry(frame, worldState, RECIPIENT)).isFalse();
  }

  @Test
  void zeroValueOrALiveRecipientConsumesNoStateGas() {
    final TopLevelTransactionSupport support = support(amsterdam);
    final WorldUpdater worldState = mock(WorldUpdater.class);

    final MessageFrame zeroValue = frameWithGas(10_000L);
    when(zeroValue.getValue()).thenReturn(Wei.ZERO);
    assertThat(support.chargeTransactionEntry(zeroValue, worldState, RECIPIENT)).isTrue();
    verify(zeroValue, never()).consumeStateGas(anyLong());

    final Account liveRecipient = mock(Account.class);
    when(worldState.get(OTHER)).thenReturn(liveRecipient);
    when(liveRecipient.getCode()).thenReturn(Bytes.EMPTY);
    final MessageFrame withValue = frameWithGas(10_000L);
    when(withValue.getValue()).thenReturn(Wei.ONE);
    assertThat(support.chargeTransactionEntry(withValue, worldState, OTHER)).isTrue();
    verify(withValue, never()).consumeStateGas(anyLong());
    verify(withValue, never()).decrementRemainingGas(anyLong());
  }

  @Test
  void coldDelegationTargetIsChargedAndListed() {
    final MessageFrame frame = frameWithGas(10_000L);
    final Eip7928AccessList accessList = mock(Eip7928AccessList.class);
    final WorldUpdater worldState = worldWithDelegatedRecipient();
    when(frame.getValue()).thenReturn(Wei.ZERO);
    when(frame.warmUpAddress(TARGET)).thenReturn(false);
    when(frame.getEip7928AccessList()).thenReturn(Optional.of(accessList));

    assertThat(support(amsterdam).chargeTransactionEntry(frame, worldState, RECIPIENT)).isTrue();
    verify(frame).decrementRemainingGas(amsterdam.getColdAccountAccessCost());
    verify(accessList).addTouchedAccount(TARGET);
  }

  @Test
  void warmDelegationTargetIsChargedTheWarmCost() {
    final MessageFrame frame = frameWithGas(10_000L);
    final WorldUpdater worldState = worldWithDelegatedRecipient();
    when(frame.getValue()).thenReturn(Wei.ZERO);
    when(frame.warmUpAddress(TARGET)).thenReturn(true);

    assertThat(support(amsterdam).chargeTransactionEntry(frame, worldState, RECIPIENT)).isTrue();
    verify(frame).decrementRemainingGas(amsterdam.getWarmStorageReadCost());
  }

  @Test
  void unaffordableDelegationTargetIsLeftOutOfTheAccessList() {
    final MessageFrame frame = frameWithGas(amsterdam.getColdAccountAccessCost() - 1);
    final Eip7928AccessList accessList = mock(Eip7928AccessList.class);
    final WorldUpdater worldState = worldWithDelegatedRecipient();
    when(frame.getValue()).thenReturn(Wei.ZERO);
    when(frame.warmUpAddress(TARGET)).thenReturn(false);
    when(frame.getEip7928AccessList()).thenReturn(Optional.of(accessList));

    assertThat(support(amsterdam).chargeTransactionEntry(frame, worldState, RECIPIENT)).isFalse();
    verify(frame, never()).decrementRemainingGas(anyLong());
    verifyNoInteractions(accessList);
  }

  // settleSelfDestructs

  @Test
  void balancePreservingForkClearsSelfDestructedAccounts() {
    final WorldUpdater worldState = mock(WorldUpdater.class);
    final MutableAccount account = mock(MutableAccount.class);
    when(worldState.getAccount(RECIPIENT)).thenReturn(account);
    when(worldState.getAccount(OTHER)).thenReturn(null);

    support(amsterdam).settleSelfDestructs(worldState, Set.of(RECIPIENT, OTHER));

    verify(account).setNonce(0L);
    verify(account).setCode(Bytes.EMPTY);
    verify(account).clearStorage();
    verify(account, never()).setBalance(any());
    verify(worldState, never()).deleteAccount(any());
  }

  @Test
  void earlierForksDeleteSelfDestructedAccounts() {
    final WorldUpdater worldState = mock(WorldUpdater.class);

    support(london).settleSelfDestructs(worldState, Set.of(RECIPIENT, OTHER));

    verify(worldState).deleteAccount(RECIPIENT);
    verify(worldState).deleteAccount(OTHER);
    verify(worldState, never()).getAccount(any());
  }

  private static Code noDelegation(final Account account) {
    throw new AssertionError("delegated code must not be resolved");
  }

  private static ProcessableBlockHeader header(final Optional<Wei> baseFee) {
    final ProcessableBlockHeader header = mock(ProcessableBlockHeader.class);
    when(header.getBaseFee()).thenReturn(baseFee);
    return header;
  }

  private static MessageFrame frameWithGas(final long remainingGas) {
    final MessageFrame frame = mock(MessageFrame.class);
    when(frame.getRemainingGas()).thenReturn(remainingGas);
    return frame;
  }

  private static WorldUpdater worldWithDelegatedRecipient() {
    final WorldUpdater worldState = mock(WorldUpdater.class);
    final Account recipient = mock(Account.class);
    when(worldState.get(RECIPIENT)).thenReturn(recipient);
    when(recipient.getCode()).thenReturn(DELEGATION_CODE);
    return worldState;
  }
}
