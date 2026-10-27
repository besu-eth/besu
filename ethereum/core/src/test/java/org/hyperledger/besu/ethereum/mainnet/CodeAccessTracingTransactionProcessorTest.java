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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.generateKeyPair;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/** The code access events {@link MainnetTransactionProcessor} emits for EIP-7702 delegations. */
class CodeAccessTracingTransactionProcessorTest {

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/mainnet/genesis-bp-it.json";
  private static final BigInteger CHAIN_ID = BigInteger.valueOf(42);

  // Funded genesis accounts, neither with code.
  private static final KeyPair SENDER =
      generateKeyPair("c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3");
  private static final KeyPair EXISTING_AUTHORITY =
      generateKeyPair("fc5141e75bf622179f8eedada7fab3e2e6b3e3da8eb9df4f46d84df22df7430e");

  private static final Address DELEGATE = Address.fromHexString("0xde1e9a7e");
  private static final Address RECIPIENT = Address.fromHexString("0x1234");

  private static final Address DELEGATED = Address.fromHexString("0xd1");
  private static final Bytes DESIGNATOR =
      Bytes.concatenate(Bytes.fromHexString("0xef0100"), DELEGATE.getBytes());
  private static final Bytes TARGET_CODE = Bytes.fromHexString("0x00");

  @Test
  void preparationOutOfGasDiscardsTheDelegationsAlreadyWritten() {
    final ExecutionContextTestFixture ctx =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final BlockHeader header = blockHeader();
    final ProtocolSpec spec = ctx.getProtocolSchedule().getByBlockHeader(header);
    final MainnetTransactionProcessor processor = spec.getTransactionProcessor();

    // The first authority exists, so it is charged ACCOUNT_WRITE and AUTH_BASE; the second is a
    // fresh account, also charged for its new leaf.
    final KeyPair newAuthority = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final Address existingAuthority = address(EXISTING_AUTHORITY);
    final Address freshAuthority = address(newAuthority);
    final Transaction unsized = delegationTransaction(newAuthority, 0L);
    // Exactly enough for the intrinsic cost and the first authority's charges, so the second
    // authority's charge runs out of gas after the first delegation was written.
    final long gasLimit =
        spec.getGasCalculator().transactionIntrinsicExecutionGas(unsized)
            + spec.getGasCalculator().getAccountWriteGasCost()
            + spec.getGasCalculator().stateGasCostCalculator().authBaseStateGas();
    final Transaction transaction = delegationTransaction(newAuthority, gasLimit);

    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final WorldUpdater updater = worldState.updater();
    final RecordingTracer tracer = new RecordingTracer();

    final TransactionProcessingResult result =
        processor.processTransaction(
            updater,
            header,
            transaction,
            Address.ZERO,
            tracer,
            mock(BlockHashLookup.class),
            Wei.ZERO);

    final Hash designator =
        Hash.hash(Bytes.concatenate(Bytes.fromHexString("0xef0100"), DELEGATE.getBytes()));
    assertThat(result.isSuccessful()).isFalse();
    assertThat(tracer.events)
        .containsExactly(
            "read " + address(SENDER) + " " + Hash.EMPTY,
            "read " + existingAuthority + " " + Hash.EMPTY,
            "write " + existingAuthority + " " + designator,
            "read " + freshAuthority + " " + Hash.EMPTY,
            "startTransactionFailed");
    // The write the tracer was told to discard never reached the state.
    assertThat(updater.get(existingAuthority).getCode()).isEqualTo(Bytes.EMPTY);
  }

  @Test
  void topLevelCallToDelegatedAccountReadsTheTargetBeforeAmsterdam() {
    // The top-level target access is not charged before Amsterdam, so its read is reported where
    // the target is resolved.
    final ExecutionContextTestFixture ctx = osakaContext();
    final BlockHeader header = blockHeader();
    final ProtocolSpec spec = osakaSpec(ctx, header);

    // The frame executes the target's code, so the target's read is reported along with the
    // recipient's designator, although no CALL opcode resolves the delegation.
    assertThat(callDelegatedAccount(ctx, header, spec))
        .containsExactlyInAnyOrder(
            "read " + address(SENDER) + " " + Hash.EMPTY,
            "read " + DELEGATED + " " + Hash.hash(DESIGNATOR),
            "read " + DELEGATE + " " + Hash.hash(TARGET_CODE));
  }

  @Test
  void topLevelCallToDelegatedAccountReadsTheTargetOnceItsAccessIsPaid() {
    // From Amsterdam the top-level target access is charged, and its read is reported once paid.
    final ExecutionContextTestFixture ctx =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final BlockHeader header = blockHeader();
    final ProtocolSpec spec = ctx.getProtocolSchedule().getByBlockHeader(header);
    assertThat(spec.getHardforkId()).isEqualTo(HardforkId.MainnetHardforkId.AMSTERDAM);

    assertThat(callDelegatedAccount(ctx, header, spec))
        .containsExactly(
            "read " + address(SENDER) + " " + Hash.EMPTY,
            "read " + DELEGATED + " " + Hash.hash(DESIGNATOR),
            "read " + DELEGATE + " " + Hash.hash(TARGET_CODE));
  }

  @Test
  void delegationsBeforeAmsterdamReportTheirCodeAccesses() {
    // No top-frame charge replays the authorizations before Amsterdam, so each one's code read
    // and delegation write are reported as they are applied.
    final ExecutionContextTestFixture ctx = osakaContext();
    final BlockHeader header = blockHeader();
    final ProtocolSpec spec = osakaSpec(ctx, header);
    final KeyPair newAuthority = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    final Transaction transaction = delegationTransaction(newAuthority, 200_000L);
    final WorldUpdater updater = ctx.getStateArchive().getWorldState().updater();
    final RecordingTracer tracer = new RecordingTracer();

    final TransactionProcessingResult result =
        spec.getTransactionProcessor()
            .processTransaction(
                updater,
                header,
                transaction,
                Address.ZERO,
                tracer,
                mock(BlockHashLookup.class),
                Wei.ZERO);

    final Hash designator =
        Hash.hash(Bytes.concatenate(Bytes.fromHexString("0xef0100"), DELEGATE.getBytes()));
    assertThat(result.isSuccessful()).isTrue();
    assertThat(tracer.events)
        .containsExactly(
            "read " + address(SENDER) + " " + Hash.EMPTY,
            "read " + address(EXISTING_AUTHORITY) + " " + Hash.EMPTY,
            "write " + address(EXISTING_AUTHORITY) + " " + designator,
            "read " + address(newAuthority) + " " + Hash.EMPTY,
            "write " + address(newAuthority) + " " + designator);
  }

  /**
   * Sends a plain transaction to {@link #DELEGATED}, an account delegated to {@link #DELEGATE},
   * which holds {@link #TARGET_CODE}, and returns the code access events.
   */
  private static List<String> callDelegatedAccount(
      final ExecutionContextTestFixture ctx, final BlockHeader header, final ProtocolSpec spec) {
    final WorldUpdater updater = ctx.getStateArchive().getWorldState().updater();
    updater.getOrCreate(DELEGATED).setCode(DESIGNATOR);
    updater.getOrCreate(DELEGATE).setCode(TARGET_CODE);
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.EIP1559)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.ONE)
            .maxFeePerGas(Wei.of(10))
            .gasLimit(100_000L)
            .to(DELEGATED)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .chainId(CHAIN_ID)
            .signAndBuild(SENDER);
    final RecordingTracer tracer = new RecordingTracer();

    final TransactionProcessingResult result =
        spec.getTransactionProcessor()
            .processTransaction(
                updater,
                header,
                transaction,
                Address.ZERO,
                tracer,
                mock(BlockHashLookup.class),
                Wei.ZERO);

    assertThat(result.isSuccessful()).isTrue();
    return tracer.events;
  }

  /** The same chain without Amsterdam, so Osaka is its last fork. */
  private static ExecutionContextTestFixture osakaContext() {
    final ObjectNode osakaGenesis =
        JsonUtil.objectNodeFromURL(
            CodeAccessTracingTransactionProcessorTest.class.getResource(GENESIS_RESOURCE), false);
    ((ObjectNode) osakaGenesis.get("config")).remove("amsterdamTime");
    return ExecutionContextTestFixture.builder(GenesisConfig.fromConfig(osakaGenesis))
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .build();
  }

  private static ProtocolSpec osakaSpec(
      final ExecutionContextTestFixture ctx, final BlockHeader header) {
    final ProtocolSpec spec = ctx.getProtocolSchedule().getByBlockHeader(header);
    assertThat(spec.getHardforkId()).isEqualTo(HardforkId.MainnetHardforkId.OSAKA);
    return spec;
  }

  private static BlockHeader blockHeader() {
    return new BlockHeaderTestFixture()
        .number(1)
        .timestamp(1)
        .gasLimit(30_000_000L)
        .baseFeePerGas(Wei.ONE)
        .buildHeader();
  }

  private static Transaction delegationTransaction(final KeyPair newAuthority, final long gas) {
    final List<CodeDelegation> codeDelegations =
        List.of(delegation(EXISTING_AUTHORITY), delegation(newAuthority));
    return Transaction.builder()
        .type(TransactionType.DELEGATE_CODE)
        .nonce(0)
        .maxPriorityFeePerGas(Wei.ONE)
        .maxFeePerGas(Wei.of(10))
        .gasLimit(gas)
        .to(RECIPIENT)
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .chainId(CHAIN_ID)
        .codeDelegations(codeDelegations)
        .signAndBuild(SENDER);
  }

  private static CodeDelegation delegation(final KeyPair authority) {
    return org.hyperledger.besu.ethereum.core.CodeDelegation.builder()
        .chainId(CHAIN_ID)
        .address(DELEGATE)
        .nonce(0)
        .signAndBuild(authority);
  }

  private static Address address(final KeyPair keyPair) {
    return Address.extract(keyPair.getPublicKey());
  }

  /** Records the code access events and the failure of the transaction's top-frame charges. */
  private static final class RecordingTracer implements OperationTracer {
    final List<String> events = new ArrayList<>();

    @Override
    public boolean isCodeAccessTracing() {
      return true;
    }

    @Override
    public void traceCodeRead(final Address address, final Hash codeHash) {
      events.add("read " + address + " " + codeHash);
    }

    @Override
    public void traceCodeWrite(final Address address, final Hash codeHash) {
      events.add("write " + address + " " + codeHash);
    }

    @Override
    public void traceStartTransactionFailed(
        final WorldView worldView, final org.hyperledger.besu.datatypes.Transaction transaction) {
      events.add("startTransactionFailed");
    }
  }
}
