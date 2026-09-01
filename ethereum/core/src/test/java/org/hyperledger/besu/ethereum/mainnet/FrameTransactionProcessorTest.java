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

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameReceipt;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** End-to-end EIP-8141 frame transaction execution against a real Bogota protocol spec. */
class FrameTransactionProcessorTest {

  private static final SignatureAlgorithm ALGO = SignatureAlgorithmFactory.getInstance();
  private static final BigInteger CHAIN_ID = BigInteger.ONE;
  private static final Wei BASE_FEE = Wei.of(7);
  private static final Wei MAX_FEE = Wei.of(100);
  private static final Wei PRIORITY_FEE = Wei.of(1);
  private static final long EXEC_LIMIT = 100_000L;
  private static final long STATE_LIMIT = 200_000L;
  private static final long NEW_ACCOUNT_STATE_GAS = 120L * 1530L;

  private static final Bytes REVERTING_CODE = Bytes.fromHexString("0x5f5ffd");
  // SSTORE(0, 1); STOP
  private static final Bytes STORE_ONE_CODE = Bytes.fromHexString("0x60015f5500");
  // APPROVE(offset=0, length=0, scope=APPROVE_PAYMENT)
  private static final Bytes PAYMASTER_CODE = Bytes.fromHexString("0x60015f5faa");

  private final KeyPair senderKeys = ALGO.generateKeyPair();
  private final Address sender =
      Address.extract(
          org.apache.tuweni.bytes.Bytes32.wrap(
              Hash.hash(senderKeys.getPublicKey().getEncodedBytes()).getBytes()));
  private final Address recipient =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private final Address contractAddress =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  private final Address secondContractAddress =
      Address.fromHexString("0x3000000000000000000000000000000000000003");
  private final Address coinbase =
      Address.fromHexString("0x4200000000000000000000000000000000000042");

  private ProtocolSchedule protocolSchedule;
  private BlockHeader blockHeader;
  private MutableWorldState worldState;

  @SuppressWarnings("UnnecessaryLambda")
  private final BlockHashLookup blockHashLookup = (frame, number) -> Hash.ZERO;

  @BeforeEach
  void setUp() {
    protocolSchedule =
        MainnetProtocolSchedule.fromConfig(
            new org.hyperledger.besu.config.StubGenesisConfigOptions()
                .bogotaTime(0)
                .chainId(CHAIN_ID),
            Optional.of(false),
            Optional.of(EvmConfiguration.DEFAULT),
            MiningConfiguration.newDefault(),
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    blockHeader =
        new BlockHeaderTestFixture()
            .number(10)
            .timestamp(1000)
            .gasLimit(30_000_000L)
            .baseFeePerGas(BASE_FEE)
            .buildHeader();
    worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater setup = worldState.updater();
    setup.createAccount(sender, 0, Wei.of(10).multiply(Wei.of(10).pow(18)));
    setup.commit();
  }

  private MainnetTransactionProcessor processor() {
    return protocolSchedule.getByBlockHeader(blockHeader).getTransactionProcessor();
  }

  private TransactionProcessingResult process(final Transaction transaction) {
    final WorldUpdater updater = worldState.updater();
    final TransactionProcessingResult result =
        processor()
            .processTransaction(
                updater,
                blockHeader,
                transaction,
                coinbase,
                OperationTracer.NO_TRACING,
                blockHashLookup,
                TransactionValidationParams.processingBlock(),
                Wei.ZERO);
    if (result.isSuccessful()) {
      updater.commit();
    }
    return result;
  }

  private Transaction buildFrameTransaction(
      final long nonce, final List<Frame> frames, final List<FrameSignature> signatures) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonce(nonce)
        .sender(sender)
        .frames(frames)
        .frameSignatures(signatures)
        .maxPriorityFeePerGas(PRIORITY_FEE)
        .maxFeePerGas(MAX_FEE)
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  /** Builds and canonically signs a frame transaction with a single SECP256K1 entry. */
  private Transaction signedFrameTransaction(final long nonce, final List<Frame> frames) {
    final FrameSignature placeholder =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 0, 65));
    final Transaction unsigned = buildFrameTransaction(nonce, frames, List.of(placeholder));
    final SECPSignature signature =
        ALGO.sign(
            org.apache.tuweni.bytes.Bytes32.wrap(unsigned.getFrameSignatureHash().getBytes()),
            senderKeys);
    final Bytes rawSignature =
        Bytes.concatenate(
            Bytes.of(signature.getRecId()),
            UInt256.valueOf(signature.getR()),
            UInt256.valueOf(signature.getS()));
    return buildFrameTransaction(
        nonce,
        frames,
        List.of(
            new FrameSignature(
                FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, rawSignature)));
  }

  private static Frame selfVerifyFrame() {
    return new Frame(
        Frame.MODE_VERIFY,
        Frame.APPROVE_SCOPE_MASK,
        Optional.empty(),
        EXEC_LIMIT,
        STATE_LIMIT,
        Wei.ZERO,
        Bytes.EMPTY);
  }

  private static Frame senderFrame(final Address target, final Wei value) {
    return new Frame(
        Frame.MODE_SENDER, 0, Optional.of(target), EXEC_LIMIT, STATE_LIMIT, value, Bytes.EMPTY);
  }

  @Test
  void transferWithDefaultCodePaysFromSenderAndCreatesRecipient() {
    final Wei transferValue = Wei.of(123_456);
    final Transaction transaction =
        signedFrameTransaction(
            0, List.of(selfVerifyFrame(), senderFrame(recipient, transferValue)));

    final Wei senderBalanceBefore = worldState.get(sender).getBalance();
    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    final var outcome = result.getFrameTransactionOutcome().orElseThrow();
    assertThat(outcome.payer().getBytes()).isEqualTo(sender.getBytes());
    assertThat(outcome.frameReceipts()).hasSize(2);

    final FrameReceipt verifyReceipt = outcome.frameReceipts().get(0);
    assertThat(verifyReceipt.status()).isEqualTo(FrameReceipt.STATUS_SUCCESS);
    // The sender is pre-warmed, so the default code's only charge is a warm entry access.
    assertThat(verifyReceipt.executionGasUsed()).isEqualTo(100L);
    assertThat(verifyReceipt.stateGasUsed()).isZero();

    final FrameReceipt senderReceipt = outcome.frameReceipts().get(1);
    assertThat(senderReceipt.status()).isEqualTo(FrameReceipt.STATUS_SUCCESS);
    // Cold entry access for the recipient.
    assertThat(senderReceipt.executionGasUsed()).isEqualTo(3000L);
    // The value transfer materialised the recipient account.
    assertThat(senderReceipt.stateGasUsed()).isEqualTo(NEW_ACCOUNT_STATE_GAS);

    assertThat(worldState.get(recipient).getBalance()).isEqualTo(transferValue);
    assertThat(worldState.get(sender).getNonce()).isEqualTo(1L);

    // Fee accounting: gasUsed = intrinsic + frame gas at the effective price of baseFee+priority.
    final long intrinsicGas =
        org.hyperledger.besu.ethereum.core.FrameTransactionGas.intrinsicGas(
            transaction.getFrames().orElseThrow(),
            transaction.getFrameSignatures().orElseThrow(),
            sender);
    final long calldataFloorGas =
        org.hyperledger.besu.ethereum.core.FrameTransactionGas.calldataFloorGas(
            transaction.getFrames().orElseThrow(),
            transaction.getFrameSignatures().orElseThrow(),
            sender);
    // The EIP-7623/7976 calldata floor binds the execution dimension when it exceeds the actual
    // execution gas (the 65 signature bytes price higher on the floor).
    final long expectedGasUsed =
        Math.max(intrinsicGas + 100L + 3000L, calldataFloorGas) + NEW_ACCOUNT_STATE_GAS;
    assertThat(result.getEstimateGasUsedByTransaction()).isEqualTo(expectedGasUsed);
    assertThat(result.getStateGasUsed()).isEqualTo(NEW_ACCOUNT_STATE_GAS);

    final Wei effectivePrice = BASE_FEE.add(PRIORITY_FEE);
    final Wei expectedFee = effectivePrice.multiply(expectedGasUsed);
    assertThat(worldState.get(sender).getBalance())
        .isEqualTo(senderBalanceBefore.subtract(transferValue).subtract(expectedFee));
    assertThat(worldState.get(coinbase).getBalance())
        .isEqualTo(PRIORITY_FEE.multiply(expectedGasUsed));
  }

  @Test
  void paymasterContractPaysForTheTransaction() {
    final WorldUpdater setup = worldState.updater();
    final MutableAccount paymaster =
        setup.createAccount(contractAddress, 0, Wei.of(10).multiply(Wei.of(10).pow(18)));
    paymaster.setCode(PAYMASTER_CODE);
    setup.commit();
    final Wei paymasterBalanceBefore = worldState.get(contractAddress).getBalance();
    final Wei senderBalanceBefore = worldState.get(sender).getBalance();

    final Frame onlyVerify =
        new Frame(
            Frame.MODE_VERIFY,
            Frame.FLAG_APPROVE_EXECUTION,
            Optional.empty(),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Frame payFrame =
        new Frame(
            Frame.MODE_VERIFY,
            Frame.FLAG_APPROVE_PAYMENT,
            Optional.of(contractAddress),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Frame userOp = senderFrame(recipient, Wei.of(1000));
    final Transaction transaction =
        signedFrameTransaction(0, List.of(onlyVerify, payFrame, userOp));

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    final var outcome = result.getFrameTransactionOutcome().orElseThrow();
    assertThat(outcome.payer().getBytes()).isEqualTo(contractAddress.getBytes());
    assertThat(outcome.frameReceipts())
        .allMatch(receipt -> receipt.status() == FrameReceipt.STATUS_SUCCESS);

    // The paymaster covered the fees; the sender only paid the transferred value.
    assertThat(worldState.get(sender).getBalance())
        .isEqualTo(senderBalanceBefore.subtract(Wei.of(1000)));
    assertThat(worldState.get(contractAddress).getBalance()).isLessThan(paymasterBalanceBefore);
    assertThat(worldState.get(sender).getNonce()).isEqualTo(1L);
  }

  @Test
  void atomicBatchFailureRollsBackExecutedFramesAndSkipsRemaining() {
    final WorldUpdater setup = worldState.updater();
    final MutableAccount store = setup.createAccount(contractAddress, 0, Wei.ZERO);
    store.setCode(STORE_ONE_CODE);
    final MutableAccount reverter = setup.createAccount(secondContractAddress, 0, Wei.ZERO);
    reverter.setCode(REVERTING_CODE);
    setup.commit();

    final Frame batchedStore =
        new Frame(
            Frame.MODE_DEFAULT,
            Frame.ATOMIC_BATCH_FLAG,
            Optional.of(contractAddress),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Frame batchedRevert =
        new Frame(
            Frame.MODE_DEFAULT,
            0,
            Optional.of(secondContractAddress),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Transaction transaction =
        signedFrameTransaction(0, List.of(selfVerifyFrame(), batchedStore, batchedRevert));

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    final var outcome = result.getFrameTransactionOutcome().orElseThrow();
    assertThat(outcome.frameReceipts().get(1).status()).isEqualTo(FrameReceipt.STATUS_SUCCESS);
    // The unrolled frame retains its execution gas but its state gas and logs are discarded.
    assertThat(outcome.frameReceipts().get(1).executionGasUsed()).isGreaterThan(0L);
    assertThat(outcome.frameReceipts().get(1).stateGasUsed()).isZero();
    assertThat(outcome.frameReceipts().get(2).status()).isEqualTo(FrameReceipt.STATUS_FAILURE);

    // The batched store was rolled back.
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
  }

  @Test
  void crossFrameStorageRefillReattributesStateGasToTheChargingFrame() {
    // One contract whose code toggles slot 0: sets it to 1 when zero, clears it when set. Frame 1
    // therefore charges the SSTORE state gas and frame 2 triggers the refill of frame 1's charge.
    final Bytes toggleCode = Bytes.fromHexString("0x5f54600114600d5760015f55005b5f5f5500");
    final WorldUpdater setup = worldState.updater();
    final MutableAccount toggle = setup.createAccount(contractAddress, 0, Wei.ZERO);
    toggle.setCode(toggleCode);
    setup.commit();

    final Frame setFrame =
        new Frame(
            Frame.MODE_DEFAULT,
            0,
            Optional.of(contractAddress),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Frame clearFrame =
        new Frame(
            Frame.MODE_DEFAULT,
            0,
            Optional.of(contractAddress),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Transaction transaction =
        signedFrameTransaction(0, List.of(selfVerifyFrame(), setFrame, clearFrame));

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    final var outcome = result.getFrameTransactionOutcome().orElseThrow();
    assertThat(outcome.frameReceipts().get(1).status()).isEqualTo(FrameReceipt.STATUS_SUCCESS);
    assertThat(outcome.frameReceipts().get(2).status()).isEqualTo(FrameReceipt.STATUS_SUCCESS);
    // The cross-frame refill removed the outstanding SSTORE charge from frame 1's receipt without
    // crediting frame 2's pool.
    assertThat(outcome.frameReceipts().get(1).stateGasUsed()).isZero();
    assertThat(outcome.frameReceipts().get(2).stateGasUsed()).isZero();
    assertThat(result.getStateGasUsed()).isZero();
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
  }

  @Test
  void introspectionOpcodesExposeTransactionAndFrameContext() {
    // SSTORE(0, TXPARAM(0x00)); SSTORE(1, FRAMEPARAM(frameIndex=0, param=0x02 mode)); STOP
    final Bytes introspectingCode = Bytes.fromHexString("0x5fb05f5560025fb360015500");
    final WorldUpdater setup = worldState.updater();
    final MutableAccount introspector = setup.createAccount(contractAddress, 0, Wei.ZERO);
    introspector.setCode(introspectingCode);
    setup.commit();

    final Frame introspectingFrame =
        new Frame(
            Frame.MODE_DEFAULT,
            0,
            Optional.of(contractAddress),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Transaction transaction =
        signedFrameTransaction(0, List.of(selfVerifyFrame(), introspectingFrame));

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    // TXPARAM(0x00) returns the frame transaction type.
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.valueOf(6));
    // FRAMEPARAM(0, 0x02) returns frame 0's mode (VERIFY).
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ONE))
        .isEqualTo(UInt256.valueOf(Frame.MODE_VERIFY));
  }

  @Test
  void expiryVerifierFrameAcceptsFutureDeadlineAndRejectsPastDeadline() {
    // Install the canonical EIP-8141 expiry verifier runtime code at 0x8141.
    final Address expiryVerifier = MainnetTransactionValidator.EXPIRY_VERIFIER;
    final Bytes expiryVerifierCode =
        Bytes.fromHexString("0x60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd");
    final WorldUpdater setup = worldState.updater();
    final MutableAccount verifier = setup.createAccount(expiryVerifier, 0, Wei.ZERO);
    verifier.setCode(expiryVerifierCode);
    setup.commit();

    // Block timestamp is 1000; a deadline of 2000 passes, a deadline of 500 fails.
    final Transaction valid =
        signedFrameTransaction(
            0,
            List.of(
                expiryFrame(expiryVerifier, 2000L),
                selfVerifyFrame(),
                senderFrame(recipient, Wei.ZERO)));
    assertThat(process(valid).isSuccessful()).isTrue();

    final Transaction expired =
        signedFrameTransaction(
            1,
            List.of(
                expiryFrame(expiryVerifier, 500L),
                selfVerifyFrame(),
                senderFrame(recipient, Wei.ZERO)));
    final TransactionProcessingResult result = process(expired);
    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.FRAME_VERIFICATION_FAILED);
  }

  private static Frame expiryFrame(final Address expiryVerifier, final long deadline) {
    return new Frame(
        Frame.MODE_VERIFY,
        0,
        Optional.of(expiryVerifier),
        EXEC_LIMIT,
        0L,
        Wei.ZERO,
        Bytes.ofUnsignedLong(deadline));
  }

  @Test
  void senderFrameWithoutApprovalInvalidatesTheTransaction() {
    final Transaction transaction =
        signedFrameTransaction(0, List.of(senderFrame(recipient, Wei.ZERO)));
    final TransactionProcessingResult result = process(transaction);
    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.FRAME_SENDER_NOT_APPROVED);
  }

  @Test
  void missingPaymentApprovalInvalidatesTheTransaction() {
    final Frame defaultFrame =
        new Frame(
            Frame.MODE_DEFAULT,
            0,
            Optional.of(recipient),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Transaction transaction = signedFrameTransaction(0, List.of(defaultFrame));
    final TransactionProcessingResult result = process(transaction);
    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.FRAME_PAYMENT_NOT_APPROVED);
  }

  @Test
  void failingVerifyFrameInvalidatesTheTransaction() {
    final WorldUpdater setup = worldState.updater();
    final MutableAccount reverter = setup.createAccount(secondContractAddress, 0, Wei.ZERO);
    reverter.setCode(REVERTING_CODE);
    setup.commit();

    final Frame failingVerify =
        new Frame(
            Frame.MODE_VERIFY,
            0,
            Optional.of(secondContractAddress),
            EXEC_LIMIT,
            STATE_LIMIT,
            Wei.ZERO,
            Bytes.EMPTY);
    final Transaction transaction =
        signedFrameTransaction(0, List.of(selfVerifyFrame(), failingVerify));
    final TransactionProcessingResult result = process(transaction);
    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.FRAME_VERIFICATION_FAILED);
  }

  @Test
  void invalidSignatureInvalidatesTheTransaction() {
    final FrameSignature badSignature =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1,
            Bytes.EMPTY,
            Bytes.EMPTY,
            Bytes.concatenate(Bytes.of(0), UInt256.valueOf(1), UInt256.valueOf(1)));
    final Transaction transaction =
        buildFrameTransaction(0, List.of(selfVerifyFrame()), List.of(badSignature));
    final TransactionProcessingResult result = process(transaction);
    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INVALID_SIGNATURE);
  }

  @Test
  void wrongNonceInvalidatesTheTransaction() {
    final Transaction transaction =
        signedFrameTransaction(5, List.of(selfVerifyFrame(), senderFrame(recipient, Wei.ZERO)));
    final TransactionProcessingResult result = process(transaction);
    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.NONCE_TOO_HIGH);
  }
}
