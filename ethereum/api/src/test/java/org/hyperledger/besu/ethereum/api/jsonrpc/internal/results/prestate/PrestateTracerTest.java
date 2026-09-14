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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.prestate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.BalanceOperation;
import org.hyperledger.besu.evm.operation.CallOperation;
import org.hyperledger.besu.evm.operation.CreateOperation;
import org.hyperledger.besu.evm.operation.SLoadOperation;
import org.hyperledger.besu.evm.operation.SelfDestructOperation;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit tests for {@link PrestateTracer} covering behaviour the Geth-captured JSON-RPC specs cannot
 * reach directly: halted-opcode discard, EIP-6780 self-destruct semantics, config-key field
 * omission, and constructor validation.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PrestateTracerTest {

  private static final Address SENDER = Address.fromHexString("0x1111");
  private static final Address CONTRACT_A = Address.fromHexString("0x2222");
  private static final Address RECIPIENT = Address.fromHexString("0x3333");

  private ProtocolSpec protocolSpec;
  private EVM evm;
  private WorldUpdater world;

  @BeforeEach
  void setUp() {
    protocolSpec = mock(ProtocolSpec.class);
    evm = mock(EVM.class);
    when(protocolSpec.getEvm()).thenReturn(evm);
    when(evm.getEvmVersion()).thenReturn(EvmSpecVersion.CANCUN);
    world = mock(WorldUpdater.class);
  }

  private PrestateTracer newTracer(final Map<String, Object> config) {
    return new PrestateTracer(
        new TraceOptions(TracerType.PRESTATE_TRACER, null, config), protocolSpec);
  }

  private void startTransaction(final PrestateTracer tracer, final Address to) {
    final Transaction tx = mock(Transaction.class);
    when(tx.getSender()).thenReturn(SENDER);
    when(tx.isContractCreation()).thenReturn(false);
    doReturn(Optional.of(to)).when(tx).getTo();
    doReturn(Optional.empty()).when(tx).getCodeDelegationList();
    tracer.tracePrepareTransaction(world, tx);
  }

  private Account mockAccount(
      final Address address, final Wei balance, final long nonce, final Bytes code) {
    final Account account = mock(Account.class);
    when(account.getAddress()).thenReturn(address);
    when(account.getBalance()).thenReturn(balance);
    when(account.getNonce()).thenReturn(nonce);
    when(account.getCode()).thenReturn(code);
    when(account.getCodeHash()).thenReturn(code.isEmpty() ? Hash.EMPTY : Hash.hash(code));
    return account;
  }

  private MessageFrame sloadFrame(final Address self, final UInt256 key) {
    final MessageFrame frame = mock(MessageFrame.class);
    final GasCalculator gasCalculator = mock(GasCalculator.class);
    final SLoadOperation operation = new SLoadOperation(gasCalculator);
    when(frame.getCurrentOperation()).thenReturn(operation);
    when(frame.getWorldUpdater()).thenReturn(world);
    when(frame.getRecipientAddress()).thenReturn(self);
    when(frame.stackSize()).thenReturn(1);
    when(frame.getStackItem(0)).thenReturn(key);
    return frame;
  }

  private MessageFrame selfDestructFrame(final Address self, final Address beneficiary) {
    final MessageFrame frame = mock(MessageFrame.class);
    final SelfDestructOperation operation = new SelfDestructOperation(null);
    when(frame.getCurrentOperation()).thenReturn(operation);
    when(frame.getWorldUpdater()).thenReturn(world);
    when(frame.getRecipientAddress()).thenReturn(self);
    when(frame.stackSize()).thenReturn(1);
    doReturn(beneficiary.getBytes()).when(frame).getStackItem(0);
    return frame;
  }

  private MessageFrame createFrame(final Address self) {
    final MessageFrame frame = mock(MessageFrame.class);
    final GasCalculator gasCalculator = mock(GasCalculator.class);
    final CreateOperation operation = new CreateOperation(gasCalculator);
    when(frame.getCurrentOperation()).thenReturn(operation);
    when(frame.getWorldUpdater()).thenReturn(world);
    when(frame.getRecipientAddress()).thenReturn(self);
    when(frame.stackSize()).thenReturn(3);
    when(frame.getStackItem(0)).thenReturn(UInt256.ZERO);
    when(frame.getStackItem(1)).thenReturn(UInt256.ZERO);
    when(frame.getStackItem(2)).thenReturn(UInt256.ZERO);
    return frame;
  }

  private void endTransaction(final PrestateTracer tracer) {
    tracer.traceEndTransaction(
        world, mock(Transaction.class), true, Bytes.EMPTY, List.of(), 0L, java.util.Set.of(), 0L);
  }

  @Test
  void haltedOpcodeDoesNotRecordLookup() {
    final PrestateTracer tracer = newTracer(Map.of());
    startTransaction(tracer, RECIPIENT);
    final Account contractA = mockAccount(CONTRACT_A, Wei.of(5), 1, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractA);
    when(contractA.getStorageValue(UInt256.ONE)).thenReturn(UInt256.valueOf(7));

    // Halted SLOAD: snapshot read but discarded
    MessageFrame frame = sloadFrame(CONTRACT_A, UInt256.ONE);
    when(frame.getState()).thenReturn(MessageFrame.State.EXCEPTIONAL_HALT);
    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, null);

    final PrestateTracerResult.Prestate halted =
        (PrestateTracerResult.Prestate) tracer.buildResult();
    // The discarded snapshot means CONTRACT_A was never recorded at all
    assertThat(halted.accounts()).doesNotContainKey(CONTRACT_A.toHexString());

    // Re-run with a committing SLOAD on a fresh tracer
    final PrestateTracer committingTracer = newTracer(Map.of());
    startTransaction(committingTracer, RECIPIENT);
    final Account contractAAgain = mockAccount(CONTRACT_A, Wei.of(5), 1, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractAAgain);
    when(contractAAgain.getStorageValue(UInt256.ONE)).thenReturn(UInt256.valueOf(7));
    frame = sloadFrame(CONTRACT_A, UInt256.ONE);
    when(frame.getState()).thenReturn(MessageFrame.State.CODE_EXECUTING);
    committingTracer.tracePreExecution(frame);
    committingTracer.tracePostExecution(frame, null);

    final PrestateTracerResult.Prestate committed =
        (PrestateTracerResult.Prestate) committingTracer.buildResult();
    assertThat(committed.accounts().get(CONTRACT_A.toHexString()).storage())
        .containsEntry(UInt256.ONE.toHexString(), UInt256.valueOf(7).toHexString());
  }

  @Test
  void selfDestructOmitsFromPostOnlyWhenDeleted() {
    // (a) Cancun, contract A not created in tx: SELFDESTRUCT marks it deleted only via balance diff
    PrestateTracer tracer = newTracer(Map.of("diffMode", true));
    final Account contractAInitial = mockAccount(CONTRACT_A, Wei.of(10), 1, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractAInitial);
    startTransaction(tracer, CONTRACT_A);

    final MessageFrame selfDestruct = selfDestructFrame(CONTRACT_A, SENDER);
    when(selfDestruct.getState()).thenReturn(MessageFrame.State.CODE_EXECUTING);
    tracer.tracePreExecution(selfDestruct);
    tracer.tracePostExecution(selfDestruct, null);
    final Account contractAFinal = mockAccount(CONTRACT_A, Wei.ZERO, 1, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractAFinal);
    endTransaction(tracer);

    PrestateTracerResult.Diff diff = (PrestateTracerResult.Diff) tracer.buildResult();
    assertThat(diff.post()).containsKey(CONTRACT_A.toHexString());
    assertThat(diff.post().get(CONTRACT_A.toHexString()).balance()).isEqualTo("0x0");

    // (b) Cancun, account B created in tx then self-destructed: deleted -> absent from post,
    // empty at snapshot -> pruned from pre
    tracer = newTracer(Map.of("diffMode", true));
    final Account contractAForCreate = mockAccount(CONTRACT_A, Wei.ZERO, 1, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractAForCreate);
    startTransaction(tracer, CONTRACT_A);

    final MessageFrame create = createFrame(CONTRACT_A);
    when(create.getState()).thenReturn(MessageFrame.State.CODE_EXECUTING);
    final Address createdAddress =
        Address.contractAddress(CONTRACT_A, contractAForCreate.getNonce());
    tracer.tracePreExecution(create);
    tracer.tracePostExecution(create, null);

    final MessageFrame selfDestructB = selfDestructFrame(createdAddress, SENDER);
    when(selfDestructB.getState()).thenReturn(MessageFrame.State.CODE_EXECUTING);
    tracer.tracePreExecution(selfDestructB);
    tracer.tracePostExecution(selfDestructB, null);
    final Account createdAccount = mockAccount(createdAddress, Wei.of(3), 1, Bytes.of(0x60, 0x00));
    when(world.get(createdAddress)).thenReturn(createdAccount);
    endTransaction(tracer);

    diff = (PrestateTracerResult.Diff) tracer.buildResult();
    assertThat(diff.post()).doesNotContainKey(createdAddress.toHexString());
    assertThat(diff.pre()).doesNotContainKey(createdAddress.toHexString());

    // (c) Pre-Cancun (Shanghai): SELFDESTRUCT always deletes
    when(evm.getEvmVersion()).thenReturn(EvmSpecVersion.SHANGHAI);
    tracer = newTracer(Map.of("diffMode", true));
    final Account contractAPreCancun = mockAccount(CONTRACT_A, Wei.of(10), 1, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractAPreCancun);
    startTransaction(tracer, CONTRACT_A);

    final MessageFrame selfDestructA = selfDestructFrame(CONTRACT_A, SENDER);
    when(selfDestructA.getState()).thenReturn(MessageFrame.State.CODE_EXECUTING);
    tracer.tracePreExecution(selfDestructA);
    tracer.tracePostExecution(selfDestructA, null);
    final Account contractAPreCancunFinal = mockAccount(CONTRACT_A, Wei.of(10), 1, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractAPreCancunFinal);
    endTransaction(tracer);

    diff = (PrestateTracerResult.Diff) tracer.buildResult();
    assertThat(diff.post()).doesNotContainKey(CONTRACT_A.toHexString());
    assertThat(diff.pre()).containsKey(CONTRACT_A.toHexString());
    when(evm.getEvmVersion()).thenReturn(EvmSpecVersion.CANCUN);
  }

  @Test
  void disableStorageAndDisableCodeOmitFields() {
    final PrestateTracer tracer = newTracer(Map.of("disableStorage", true, "disableCode", true));
    final Account contractA = mockAccount(CONTRACT_A, Wei.of(1), 0, Bytes.of(0x60, 0x00));
    when(world.get(CONTRACT_A)).thenReturn(contractA);
    startTransaction(tracer, CONTRACT_A);

    final MessageFrame sload = sloadFrame(CONTRACT_A, UInt256.ONE);
    when(sload.getState()).thenReturn(MessageFrame.State.CODE_EXECUTING);
    tracer.tracePreExecution(sload); // disableStorage: no-op
    tracer.tracePostExecution(sload, null);
    endTransaction(tracer);

    final PrestateTracerResult.Prestate result =
        (PrestateTracerResult.Prestate) tracer.buildResult();
    final PrestateTracerResult.Account account = result.accounts().get(CONTRACT_A.toHexString());
    assertThat(account).isNotNull();
    assertThat(account.code()).isNull();
    assertThat(account.storage()).isNull();
    // Emptiness computed before code dropped: non-empty (code + balance) so not pruned,
    // and the code hash is still reported.
    assertThat(account.codeHash()).isEqualTo(Hash.hash(Bytes.of(0x60, 0x00)).toHexString());
    assertThat(account.balance()).isEqualTo("0x1");
  }

  @Test
  void includeEmptyKeepsEmptyAccounts() {
    final PrestateTracer tracer = newTracer(Map.of("includeEmpty", true));
    when(world.get(RECIPIENT)).thenReturn(null);
    startTransaction(tracer, RECIPIENT);
    endTransaction(tracer);

    final PrestateTracerResult.Prestate result =
        (PrestateTracerResult.Prestate) tracer.buildResult();
    final PrestateTracerResult.Account account = result.accounts().get(RECIPIENT.toHexString());
    assertThat(account).isNotNull();
    assertThat(account.balance()).isEqualTo("0x0");
    assertThat(account.nonce()).isNull();
    assertThat(account.code()).isNull();
    assertThat(account.codeHash()).isNull();
    assertThat(account.storage()).isNull();
  }

  @Test
  void diffModeWithIncludeEmptyIsRejected() {
    assertThatThrownBy(() -> newTracer(Map.of("diffMode", true, "includeEmpty", true)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("cannot use diffMode with includeEmpty");
  }

  @Test
  void balanceOpcodeLooksUpTarget() {
    final PrestateTracer tracer = newTracer(Map.of());
    startTransaction(tracer, RECIPIENT);
    final Account contractA = mockAccount(CONTRACT_A, Wei.of(9), 0, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractA);

    final MessageFrame frame = mock(MessageFrame.class);
    final GasCalculator gasCalculator = mock(GasCalculator.class);
    when(frame.getCurrentOperation()).thenReturn(new BalanceOperation(gasCalculator));
    when(frame.getWorldUpdater()).thenReturn(world);
    when(frame.getRecipientAddress()).thenReturn(RECIPIENT);
    when(frame.stackSize()).thenReturn(1);
    doReturn(CONTRACT_A.getBytes()).when(frame).getStackItem(0);
    when(frame.getState()).thenReturn(MessageFrame.State.CODE_EXECUTING);

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, null);

    final PrestateTracerResult.Prestate result =
        (PrestateTracerResult.Prestate) tracer.buildResult();
    assertThat(result.accounts().get(CONTRACT_A.toHexString()).balance()).isEqualTo("0x9");
  }

  @Test
  void delegationTargetLookedUpForTxLevelTo() {
    when(evm.getEvmVersion()).thenReturn(EvmSpecVersion.PRAGUE);
    final PrestateTracer tracer = newTracer(Map.of());
    final Bytes delegatedCode =
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, CONTRACT_A.getBytes());
    final Account recipientAccount = mockAccount(RECIPIENT, Wei.ZERO, 1, delegatedCode);
    when(world.get(RECIPIENT)).thenReturn(recipientAccount);
    final Account contractA = mockAccount(CONTRACT_A, Wei.of(3), 0, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractA);

    startTransaction(tracer, RECIPIENT);

    final PrestateTracerResult.Prestate result =
        (PrestateTracerResult.Prestate) tracer.buildResult();
    assertThat(result.accounts().get(CONTRACT_A.toHexString()).balance()).isEqualTo("0x3");
  }

  @Test
  void delegationTargetLookedUpForCallTarget() {
    when(evm.getEvmVersion()).thenReturn(EvmSpecVersion.PRAGUE);
    final PrestateTracer tracer = newTracer(Map.of());
    startTransaction(tracer, RECIPIENT);
    final Bytes delegatedCode =
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, CONTRACT_A.getBytes());
    final Account recipientAccount = mockAccount(RECIPIENT, Wei.ZERO, 1, delegatedCode);
    when(world.get(RECIPIENT)).thenReturn(recipientAccount);
    final Account contractA = mockAccount(CONTRACT_A, Wei.of(4), 0, Bytes.EMPTY);
    when(world.get(CONTRACT_A)).thenReturn(contractA);

    final MessageFrame frame = mock(MessageFrame.class);
    final GasCalculator gasCalculator = mock(GasCalculator.class);
    when(frame.getCurrentOperation()).thenReturn(new CallOperation(gasCalculator));
    when(frame.getWorldUpdater()).thenReturn(world);
    when(frame.getRecipientAddress()).thenReturn(SENDER);
    when(frame.stackSize()).thenReturn(7);
    doReturn(RECIPIENT.getBytes()).when(frame).getStackItem(1);
    when(frame.getState()).thenReturn(MessageFrame.State.CODE_EXECUTING);

    tracer.tracePreExecution(frame);
    tracer.tracePostExecution(frame, null);

    final PrestateTracerResult.Prestate result =
        (PrestateTracerResult.Prestate) tracer.buildResult();
    assertThat(result.accounts().get(CONTRACT_A.toHexString()).balance()).isEqualTo("0x4");
  }

  @Test
  void authorizationListAuthoritiesLookedUp() {
    when(evm.getEvmVersion()).thenReturn(EvmSpecVersion.PRAGUE);
    final PrestateTracer tracer = newTracer(Map.of());
    final Address authority = Address.fromHexString("0x4444");
    final Account authorityAccount = mockAccount(authority, Wei.of(2), 5, Bytes.EMPTY);
    when(world.get(authority)).thenReturn(authorityAccount);
    final Account recipientAccount = mockAccount(RECIPIENT, Wei.ZERO, 0, Bytes.EMPTY);
    when(world.get(RECIPIENT)).thenReturn(recipientAccount);

    final Transaction tx = mock(Transaction.class);
    when(tx.getSender()).thenReturn(SENDER);
    when(tx.isContractCreation()).thenReturn(false);
    doReturn(Optional.of(RECIPIENT)).when(tx).getTo();
    final org.hyperledger.besu.datatypes.CodeDelegation authorization =
        mock(org.hyperledger.besu.datatypes.CodeDelegation.class);
    when(authorization.authorizer()).thenReturn(Optional.of(authority));
    doReturn(Optional.of(List.of(authorization))).when(tx).getCodeDelegationList();
    tracer.tracePrepareTransaction(world, tx);

    final PrestateTracerResult.Prestate result =
        (PrestateTracerResult.Prestate) tracer.buildResult();
    assertThat(result.accounts().get(authority.toHexString()).balance()).isEqualTo("0x2");
  }
}
