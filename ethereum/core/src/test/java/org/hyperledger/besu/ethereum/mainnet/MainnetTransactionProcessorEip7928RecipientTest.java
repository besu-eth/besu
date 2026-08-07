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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.CodeDelegationResult.AuthorityAccess;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * EIP-7928: "A transaction can halt on a runtime gas charge before the recipient is loaded (e.g.,
 * during EIP-7702 authorization processing); the recipient is then never accessed and MUST NOT be
 * included unless accessed for another reason."
 *
 * <p>EIP-2780 charges each authorization's state-dependent costs at the top frame, before dispatch
 * preparation loads the recipient. These tests pin the resulting ordering: an authorization charge
 * that runs out of gas leaves the recipient out of the block access list, while the same
 * transaction with enough gas to clear those charges reaches the load and records it.
 */
@ExtendWith(MockitoExtension.class)
class MainnetTransactionProcessorEip7928RecipientTest {

  private static final int MAX_STACK_SIZE = 1024;

  private static final Address SENDER =
      Address.fromHexString("0x5555555555555555555555555555555555555555");
  private static final Address RECIPIENT =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address AUTHORITY =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address COINBASE =
      Address.fromHexString("0x4242424242424242424242424242424242424242");

  /**
   * TX_BASE (12,000) + COLD_ACCOUNT_ACCESS (3,000) + one REGULAR_PER_AUTH_BASE_COST (7,816), the
   * intrinsic of a zero-value single-authorization transaction to another account with no payload.
   */
  private static final long INTRINSIC_GAS = 22_816L;

  /** EIP-8038 ACCOUNT_WRITE, charged per authority on the transaction's first write to its leaf. */
  private static final long ACCOUNT_WRITE = 8_000L;

  /** An authority owing only ACCOUNT_WRITE, the cheapest authorization charge that can halt. */
  private static final AuthorityAccess OWES_ACCOUNT_WRITE =
      new AuthorityAccess(AUTHORITY, false, true, false);

  private final GasCalculator gasCalculator = new AmsterdamGasCalculator();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private TransactionValidatorFactory transactionValidatorFactory;

  @Mock private ContractCreationProcessor contractCreationProcessor;
  @Mock private MessageCallProcessor messageCallProcessor;
  @Mock private CodeDelegationProcessor codeDelegationProcessor;

  @Mock private WorldUpdater worldState;
  @Mock private ProcessableBlockHeader blockHeader;
  @Mock private Transaction transaction;
  @Mock private BlockHashLookup blockHashLookup;
  @Mock private MutableAccount senderAccount;

  @BeforeEach
  void setUp() {
    lenient().when(transaction.getType()).thenReturn(TransactionType.DELEGATE_CODE);
    lenient().when(transaction.getTo()).thenReturn(Optional.of(RECIPIENT));
    lenient().when(transaction.getSender()).thenReturn(SENDER);
    lenient().when(transaction.getValue()).thenReturn(Wei.ZERO);
    lenient().when(transaction.getPayload()).thenReturn(Bytes.EMPTY);
    lenient().when(transaction.codeDelegationListSize()).thenReturn(1);
    lenient()
        .when(transactionValidatorFactory.get().validate(any(), any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    lenient()
        .when(transactionValidatorFactory.get().validateForSender(any(), any(), any()))
        .thenReturn(ValidationResult.valid());
    lenient().when(worldState.getOrCreateSenderAccount(any())).thenReturn(senderAccount);
    lenient().when(worldState.getOrCreate(any())).thenReturn(senderAccount);
    lenient().when(worldState.updater()).thenReturn(worldState);
    // The frame that reaches execution runs no code; pop it so the processing loop terminates.
    lenient()
        .doAnswer(
            invocation -> {
              invocation.<MessageFrame>getArgument(0).getMessageFrameStack().pop();
              return null;
            })
        .when(messageCallProcessor)
        .process(any(), any());
  }

  @Test
  void recipientIsExcludedWhenAnAuthorizationChargeHaltsBeforeItIsLoaded() {
    // One gas short of the authority's ACCOUNT_WRITE, so preparation halts before dispatch prep
    // loads the recipient.
    final AccessLocationTracker tracker =
        processWith(OWES_ACCOUNT_WRITE, INTRINSIC_GAS + ACCOUNT_WRITE - 1L);

    assertThat(touchedAddresses(tracker)).contains(SENDER, AUTHORITY).doesNotContain(RECIPIENT);
  }

  @Test
  void recipientIsIncludedWhenTheAuthorizationChargesAreAffordable() {
    // Exactly enough for ACCOUNT_WRITE: preparation reaches dispatch prep, which loads the
    // recipient and records it.
    final AccessLocationTracker tracker =
        processWith(OWES_ACCOUNT_WRITE, INTRINSIC_GAS + ACCOUNT_WRITE);

    assertThat(touchedAddresses(tracker)).contains(SENDER, AUTHORITY, RECIPIENT);
  }

  @Test
  void recipientIsIncludedWhenNoAuthorizationOwesARuntimeCharge() {
    // A touched-but-uncharged authority cannot halt, so the load always happens.
    final AccessLocationTracker tracker =
        processWith(AuthorityAccess.touchOnly(AUTHORITY), INTRINSIC_GAS);

    assertThat(touchedAddresses(tracker)).contains(SENDER, AUTHORITY, RECIPIENT);
  }

  private AccessLocationTracker processWith(final AuthorityAccess access, final long gasLimit) {
    when(transaction.getGasLimit()).thenReturn(gasLimit);
    final CodeDelegationResult delegationResult = new CodeDelegationResult();
    delegationResult.addAuthorityAccess(access);
    when(codeDelegationProcessor.process(any(), any())).thenReturn(delegationResult);

    final AccessLocationTracker tracker = new AccessLocationTracker(0L);
    transactionProcessor()
        .processTransaction(
            worldState,
            blockHeader,
            transaction,
            COINBASE,
            OperationTracer.NO_TRACING,
            blockHashLookup,
            ImmutableTransactionValidationParams.builder().build(),
            Wei.ZERO,
            Optional.of(tracker));
    return tracker;
  }

  private static Set<Address> touchedAddresses(final AccessLocationTracker tracker) {
    return tracker.getTouchedAccounts().stream()
        .map(AccessLocationTracker.AccountAccessList::getAddress)
        .collect(Collectors.toSet());
  }

  private MainnetTransactionProcessor transactionProcessor() {
    return MainnetTransactionProcessor.builder()
        .gasCalculator(gasCalculator)
        .transactionValidatorFactory(transactionValidatorFactory)
        .contractCreationProcessor(contractCreationProcessor)
        .messageCallProcessor(messageCallProcessor)
        .codeDelegationProcessor(codeDelegationProcessor)
        .clearEmptyAccounts(false)
        .warmCoinbase(true)
        .maxStackSize(MAX_STACK_SIZE)
        .feeMarket(FeeMarket.legacy())
        .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.frontier())
        .build();
  }
}
