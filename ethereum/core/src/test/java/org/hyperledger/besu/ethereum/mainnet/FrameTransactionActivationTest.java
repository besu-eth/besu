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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO5;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.EXPERIMENTAL_EIPS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.FUTURE_EIPS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameReceipt;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Which forks accept and execute EIP-8141 frame transactions, and that Bogota's transaction
 * validator and processor are those of the fork before it plus frame transactions.
 */
class FrameTransactionActivationTest {

  private static final BigInteger CHAIN_ID = BigInteger.valueOf(1337);
  private static final Wei BASE_FEE = Wei.of(7);

  private static final long OSAKA_TIME = 10L;
  private static final long AMSTERDAM_TIME = 20L;
  private static final long BPO5_TIME = 30L;
  private static final long BOGOTA_TIME = 40L;
  private static final long FUTURE_EIPS_TIME = 50L;
  private static final long EXPERIMENTAL_EIPS_TIME = 60L;

  // APPROVE(offset 0, length 0, scope APPROVE_EXECUTION | APPROVE_PAYMENT)
  private static final Bytes APPROVE_ALL_CODE = Bytes.fromHexString("0x60035f5faa");

  private static final Address SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address RECIPIENT =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address TRANSFER_SENDER =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address COINBASE =
      Address.fromHexString("0x4200000000000000000000000000000000000042");

  @SuppressWarnings("UnnecessaryLambda")
  private static final BlockHashLookup BLOCK_HASH_LOOKUP = (frame, number) -> Hash.ZERO;

  private static final ProtocolSchedule SCHEDULE = schedule();

  @ParameterizedTest(name = "{0}")
  @MethodSource("framesForks")
  void frameCapableForksAcceptFrameTransactions(final HardforkId fork, final long timestamp) {
    final ProtocolSpec spec = specAt(timestamp);
    assertThat(spec.getHardforkId()).isEqualTo(fork);

    assertThat(validate(spec, decodedFrameTransaction()).isValid()).isTrue();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("forksWithoutFrames")
  void forksBeforeBogotaRejectADecodedFrameTransactionAtValidation(
      final HardforkId fork, final long timestamp) {
    final ProtocolSpec spec = specAt(timestamp);
    assertThat(spec.getHardforkId()).isEqualTo(fork);

    final ValidationResult<TransactionInvalidReason> result =
        validate(spec, decodedFrameTransaction());

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INVALID_TRANSACTION_FORMAT);
    assertThat(result.getErrorMessage()).startsWith("Transaction type FRAME is invalid");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("forksWithoutFrames")
  void forksBeforeBogotaDoNotExecuteFrameTransactions(final HardforkId fork, final long timestamp) {
    final ProtocolSpec spec = specAt(timestamp);
    assertThat(spec.getHardforkId()).isEqualTo(fork);

    final TransactionProcessingResult result =
        process(spec, freshWorldState(), decodedFrameTransaction(), timestamp);

    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INVALID_TRANSACTION_FORMAT);
    assertThat(result.getValidationResult().getErrorMessage())
        .isEqualTo("frame transactions are not supported at the current fork");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("framesForks")
  void frameCapableForksExecuteFrameTransactions(final HardforkId fork, final long timestamp) {
    final ProtocolSpec spec = specAt(timestamp);
    assertThat(spec.getHardforkId()).isEqualTo(fork);
    final MutableWorldState worldState = freshWorldState();

    final TransactionProcessingResult result =
        process(spec, worldState, decodedFrameTransaction(), timestamp);

    assertThat(result.isSuccessful()).isTrue();
    final var outcome = result.getFrameTransactionOutcome().orElseThrow();
    assertThat(outcome.payer()).isEqualTo(SENDER);
    assertThat(outcome.frameReceipts())
        .extracting(FrameReceipt::status)
        .containsExactly(FrameReceipt.STATUS_SUCCESS, FrameReceipt.STATUS_SUCCESS);
    assertThat(worldState.get(SENDER).getNonce()).isEqualTo(1L);
    assertThat(worldState.get(RECIPIENT).getBalance()).isEqualTo(Wei.of(1000));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(
      value = TransactionType.class,
      names = {"FRAME"},
      mode = EnumSource.Mode.EXCLUDE)
  void bogotaValidatesEveryOtherTypeAsTheForkBeforeIt(final TransactionType type) {
    final Transaction valid = signedTransaction(type, 1_000_000L);
    final ValidationResult<TransactionInvalidReason> validOnParent =
        validate(specAt(BPO5_TIME), valid);
    assertThat(validOnParent.isValid())
        .as(() -> type + " on the fork before Bogota: " + validOnParent.getErrorMessage())
        .isTrue();
    assertThat(validate(specAt(BOGOTA_TIME), valid).isValid()).isTrue();

    // Below its intrinsic gas: the same rejection on both forks.
    final Transaction underGassed = signedTransaction(type, 1_000L);
    final ValidationResult<TransactionInvalidReason> parent =
        validate(specAt(BPO5_TIME), underGassed);
    final ValidationResult<TransactionInvalidReason> bogota =
        validate(specAt(BOGOTA_TIME), underGassed);
    assertThat(parent.isValid()).isFalse();
    assertThat(bogota.getInvalidReason()).isEqualTo(parent.getInvalidReason());
    assertThat(bogota.getErrorMessage()).isEqualTo(parent.getErrorMessage());
  }

  @Test
  void bogotaProcessesOtherTransactionsAsTheForkBeforeIt() {
    final MainnetTransactionProcessor parent = specAt(BPO5_TIME).getTransactionProcessor();
    final MainnetTransactionProcessor bogota = specAt(BOGOTA_TIME).getTransactionProcessor();

    assertThat(bogota).isNotSameAs(parent);
    assertThat(bogota.getClearEmptyAccounts()).isEqualTo(parent.getClearEmptyAccounts());
    assertThat(bogota.getTransferLogEmitter()).isSameAs(parent.getTransferLogEmitter());
    assertThat(bogota.getTransferLogEmitter()).isNotSameAs(TransferLogEmitter.NOOP);
    assertThat(bogota.getGasCalculator()).isExactlyInstanceOf(parent.getGasCalculator().getClass());
    assertThat(bogota.getMessageCallProcessor())
        .isExactlyInstanceOf(parent.getMessageCallProcessor().getClass());

    // The same EIP-1559 transfer from the same state has the same outcome on both forks.
    final Transaction transfer = signedTransaction(TransactionType.EIP1559);
    final MutableWorldState parentState = fundedWorldState(transfer.getSender());
    final MutableWorldState bogotaState = fundedWorldState(transfer.getSender());

    final TransactionProcessingResult onParent =
        process(specAt(BPO5_TIME), parentState, transfer, BPO5_TIME);
    final TransactionProcessingResult onBogota =
        process(specAt(BOGOTA_TIME), bogotaState, transfer, BOGOTA_TIME);

    assertThat(onParent.isSuccessful())
        .as(() -> onParent.getValidationResult().getErrorMessage())
        .isTrue();
    assertThat(onBogota.isSuccessful()).isTrue();
    assertThat(onBogota.getEstimateGasUsedByTransaction())
        .isEqualTo(onParent.getEstimateGasUsedByTransaction());
    assertThat(onBogota.getStateGasUsed()).isEqualTo(onParent.getStateGasUsed());
    assertThat(onBogota.getLogs()).isEqualTo(onParent.getLogs());
    assertThat(onBogota.getFrameTransactionOutcome()).isEmpty();
    assertThat(bogotaState.rootHash())
        .isEqualTo(parentState.rootHash())
        .isNotEqualTo(fundedWorldState(transfer.getSender()).rootHash());
  }

  private static Stream<Arguments> framesForks() {
    return Stream.of(
        Arguments.of(BOGOTA, BOGOTA_TIME),
        Arguments.of(FUTURE_EIPS, FUTURE_EIPS_TIME),
        Arguments.of(EXPERIMENTAL_EIPS, EXPERIMENTAL_EIPS_TIME));
  }

  private static Stream<Arguments> forksWithoutFrames() {
    return Stream.of(
        Arguments.of(OSAKA, OSAKA_TIME),
        Arguments.of(AMSTERDAM, AMSTERDAM_TIME),
        Arguments.of(BPO5, BPO5_TIME));
  }

  /**
   * A frame transaction as it arrives from the network: the sender's own code approves execution
   * and payment in a VERIFY frame, then a SENDER frame transfers value.
   */
  private static Transaction decodedFrameTransaction() {
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(CHAIN_ID)
            .nonce(0)
            .sender(SENDER)
            .frames(
                List.of(
                    new Frame(
                        Frame.MODE_VERIFY,
                        Frame.APPROVE_SCOPE_MASK,
                        Optional.empty(),
                        100_000L,
                        200_000L,
                        Wei.ZERO,
                        Bytes.EMPTY),
                    new Frame(
                        Frame.MODE_SENDER,
                        0,
                        Optional.of(RECIPIENT),
                        100_000L,
                        200_000L,
                        Wei.of(1000),
                        Bytes.EMPTY)))
            .frameSignatures(List.of())
            .maxPriorityFeePerGas(Wei.of(1))
            .maxFeePerGas(Wei.of(100))
            .maxFeePerBlobGas(Wei.ZERO)
            .build();
    return TransactionDecoder.decodeOpaqueBytes(
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY),
        EncodingContext.BLOCK_BODY);
  }

  private static Transaction signedTransaction(final TransactionType type) {
    // Covers the state gas of creating the recipient (EIP-8037).
    return signedTransaction(type, 1_000_000L);
  }

  private static Transaction signedTransaction(final TransactionType type, final long gasLimit) {
    final KeyPair keys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
    return new TransactionTestFixture()
        .type(type)
        .sender(TRANSFER_SENDER)
        .chainId(Optional.of(CHAIN_ID))
        .to(Optional.of(RECIPIENT))
        .gasLimit(gasLimit)
        .versionedHashes(
            type.supportsBlob()
                ? Optional.of(
                    List.of(new VersionedHash(VersionedHash.SHA256_VERSION_ID, Hash.ZERO)))
                : Optional.empty())
        .createTransaction(keys);
  }

  private static ValidationResult<TransactionInvalidReason> validate(
      final ProtocolSpec spec, final Transaction transaction) {
    return spec.getTransactionValidatorFactory()
        .get()
        .validate(
            transaction,
            Optional.of(BASE_FEE),
            Optional.of(Wei.ONE),
            TransactionValidationParams.processingBlock());
  }

  private static TransactionProcessingResult process(
      final ProtocolSpec spec,
      final MutableWorldState worldState,
      final Transaction transaction,
      final long timestamp) {
    final WorldUpdater updater = worldState.updater();
    final TransactionProcessingResult result =
        spec.getTransactionProcessor()
            .processTransaction(
                updater,
                header(timestamp),
                transaction,
                COINBASE,
                OperationTracer.NO_TRACING,
                BLOCK_HASH_LOOKUP,
                TransactionValidationParams.processingBlock(),
                Wei.ONE,
                Optional.empty());
    if (result.isSuccessful()) {
      updater.commit();
    }
    return result;
  }

  /** A world state in which the frame transaction's sender is a funded account with code. */
  private static MutableWorldState freshWorldState() {
    final MutableWorldState worldState = fundedWorldState(SENDER);
    final WorldUpdater setup = worldState.updater();
    setup.getAccount(SENDER).setCode(APPROVE_ALL_CODE);
    setup.commit();
    return worldState;
  }

  private static MutableWorldState fundedWorldState(final Address account) {
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater setup = worldState.updater();
    setup.createAccount(account, 0, Wei.of(10).multiply(Wei.of(10).pow(18)));
    setup.commit();
    return worldState;
  }

  private static BlockHeader header(final long timestamp) {
    return new BlockHeaderTestFixture()
        .number(1L)
        .timestamp(timestamp)
        .gasLimit(30_000_000L)
        .baseFeePerGas(BASE_FEE)
        .buildHeader();
  }

  private static ProtocolSpec specAt(final long timestamp) {
    return SCHEDULE.getByBlockHeader(header(timestamp));
  }

  private static ProtocolSchedule schedule() {
    return MainnetProtocolSchedule.fromConfig(
        new StubGenesisConfigOptions()
            .chainId(CHAIN_ID)
            .londonBlock(0L)
            .shanghaiTime(0L)
            .cancunTime(0L)
            .pragueTime(0L)
            .osakaTime(OSAKA_TIME)
            .amsterdamTime(AMSTERDAM_TIME)
            .bpo5Time(BPO5_TIME)
            .bogotaTime(BOGOTA_TIME)
            .futureEipsTime(FUTURE_EIPS_TIME)
            .experimentalEipsTime(EXPERIMENTAL_EIPS_TIME),
        EvmConfiguration.DEFAULT,
        MiningConfiguration.MINING_DISABLED,
        new BadBlockManager(),
        false,
        BalConfiguration.DEFAULT,
        new NoOpMetricsSystem());
  }
}
