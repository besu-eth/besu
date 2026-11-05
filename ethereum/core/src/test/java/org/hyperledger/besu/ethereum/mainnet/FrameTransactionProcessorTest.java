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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
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
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** End-to-end EIP-8141 frame transaction execution against a real Bogota protocol spec. */
class FrameTransactionProcessorTest {

  private static final SignatureAlgorithm ALGO = SignatureAlgorithmFactory.getInstance();
  private static final BigInteger CHAIN_ID = BigInteger.ONE;
  private static final Wei BASE_FEE = Wei.of(7);
  // A budget large enough to pay the full tip on every unit of gas.
  private static final Wei MAX_FEE = Wei.fromEth(1);
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
                Wei.ZERO,
                Optional.empty());
    if (result.isSuccessful()) {
      updater.commit();
    }
    return result;
  }

  private Transaction buildFrameTransaction(
      final long nonce, final List<Frame> frames, final List<FrameSignature> signatures) {
    return buildFrameTransaction(nonce, frames, signatures, MAX_FEE, PRIORITY_FEE);
  }

  private Transaction buildFrameTransaction(
      final long nonce,
      final List<Frame> frames,
      final List<FrameSignature> signatures,
      final Wei maxFee,
      final Wei maxPriorityFeePerGas) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonce(nonce)
        .sender(sender)
        .frames(frames)
        .frameSignatures(signatures)
        .maxPriorityFeePerGas(maxPriorityFeePerGas)
        .maxFee(maxFee)
        .build();
  }

  /** Builds and canonically signs a frame transaction with a single SECP256K1 entry. */
  private Transaction signedFrameTransaction(final long nonce, final List<Frame> frames) {
    return signedFrameTransaction(nonce, frames, MAX_FEE, PRIORITY_FEE);
  }

  private Transaction signedFrameTransaction(
      final long nonce,
      final List<Frame> frames,
      final Wei maxFee,
      final Wei maxPriorityFeePerGas) {
    final FrameSignature placeholder =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 0, 65));
    final Transaction unsigned =
        buildFrameTransaction(nonce, frames, List.of(placeholder), maxFee, maxPriorityFeePerGas);
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
                FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, rawSignature)),
        maxFee,
        maxPriorityFeePerGas);
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
        org.hyperledger.besu.ethereum.core.FrameTransactionGas.intrinsicGas(transaction);
    final long calldataFloorGas =
        org.hyperledger.besu.ethereum.core.FrameTransactionGas.calldataFloorGas(transaction);
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

  // GASPRICE; PUSH0; SSTORE; PUSH1 0x04; TXPARAM; PUSH1 1; SSTORE; STOP
  private static final Bytes FEE_INTROSPECTION_CODE = Bytes.fromHexString("0x3a5f556004b060015500");

  // PUSH1 3; PUSH0; PUSH0; APPROVE: approves execution and payment.
  private static final Bytes APPROVE_BOTH_CODE = Bytes.fromHexString("0x60035f5faa");

  private static final Wei LARGE_TIP = Wei.of(1_000);

  /**
   * A transaction whose sender approves both scopes from its own code. Without signature entries,
   * whose random bytes are priced as calldata, its max_gas does not depend on its fees.
   */
  private Transaction codeApprovedTransaction(
      final Frame body, final Wei maxFee, final Wei maxPriorityFeePerGas) {
    return buildFrameTransaction(
        0, List.of(selfVerifyFrame(), body), List.of(), maxFee, maxPriorityFeePerGas);
  }

  /**
   * The EIP-8141 fee budget, as tests-bogota-devnet@v2.0.0 test_fee_budget.py exercises it. The
   * payer escrows max_gas at the base fee plus the largest tip the rest of max_fee allows. GASPRICE
   * reports the base fee plus that tip spread over max_gas. Settlement burns the gas used at the
   * base fee and pays the coinbase the tip on the gas used, capped by what the escrow still holds.
   * The expected values follow EELS calculate_frame_transaction_fees and disburse_frame_gas_fees.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("feeBudgets")
  void feeBudgetSetsTheEscrowGasPriceAndSettlement(
      final String name,
      final Wei tip,
      final BiFunction<BigInteger, BigInteger, BigInteger> budget) {
    deploy(sender, APPROVE_BOTH_CODE);
    deploy(contractAddress, FEE_INTROSPECTION_CODE);
    final Frame body = bodyFrame(Frame.MODE_DEFAULT, 0, contractAddress);
    final BigInteger maxGas =
        BigInteger.valueOf(codeApprovedTransaction(body, Wei.ZERO, tip).getGasLimit());
    final BigInteger baseFee = BASE_FEE.getAsBigInteger();
    final BigInteger required = maxGas.multiply(baseFee);
    final BigInteger maxFee = required.add(budget.apply(maxGas, tip.getAsBigInteger()));
    final Transaction transaction = codeApprovedTransaction(body, Wei.of(maxFee), tip);
    final Wei senderBalanceBefore = worldState.get(sender).getBalance();

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    final BigInteger maxPriorityFee =
        tip.getAsBigInteger().multiply(maxGas).min(maxFee.subtract(required));
    final BigInteger maxCost = required.add(maxPriorityFee);
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.valueOf(baseFee.add(maxPriorityFee.divide(maxGas))));
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ONE))
        .isEqualTo(UInt256.valueOf(maxFee));

    final BigInteger gasUsed = BigInteger.valueOf(result.getEstimateGasUsedByTransaction());
    final BigInteger baseFeePaid = gasUsed.multiply(baseFee);
    final BigInteger priorityFeePaid =
        tip.getAsBigInteger().multiply(gasUsed).min(maxCost.subtract(baseFeePaid));
    assertThat(worldState.get(coinbase).getBalance()).isEqualTo(Wei.of(priorityFeePaid));
    assertThat(worldState.get(sender).getBalance())
        .isEqualTo(senderBalanceBefore.subtract(Wei.of(baseFeePaid.add(priorityFeePaid))));
  }

  static Stream<Arguments> feeBudgets() {
    return Stream.of(
        Arguments.of(
            "surplus budget, full tip",
            LARGE_TIP,
            (BiFunction<BigInteger, BigInteger, BigInteger>)
                (maxGas, tip) -> tip.multiply(maxGas).add(BigInteger.valueOf(12_345))),
        Arguments.of(
            "partial tip",
            LARGE_TIP,
            (BiFunction<BigInteger, BigInteger, BigInteger>)
                (maxGas, tip) -> tip.multiply(maxGas).divide(BigInteger.valueOf(4))),
        Arguments.of(
            "no headroom caps the tip at settlement",
            LARGE_TIP,
            (BiFunction<BigInteger, BigInteger, BigInteger>) (maxGas, tip) -> BigInteger.ZERO),
        Arguments.of(
            "tip of 2^256-1",
            Wei.MAX_WEI,
            (BiFunction<BigInteger, BigInteger, BigInteger>)
                (maxGas, tip) -> BigInteger.valueOf(1_000)));
  }

  @Test
  void maxFeeBelowTheRequiredFeeInvalidatesTheTransaction() {
    deploy(sender, APPROVE_BOTH_CODE);
    final Frame body = senderFrame(recipient, Wei.ZERO);
    final long maxGas = codeApprovedTransaction(body, Wei.ZERO, PRIORITY_FEE).getGasLimit();
    final Transaction transaction =
        codeApprovedTransaction(body, BASE_FEE.multiply(maxGas).subtract(1), PRIORITY_FEE);

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.GAS_PRICE_BELOW_CURRENT_BASE_FEE);
    assertThat(result.getValidationResult().getErrorMessage())
        .startsWith("gasPrice is less than the current BaseFee");
  }

  @Test
  void expiryVerifierFrameAcceptsFutureDeadlineAndRejectsPastDeadline() {
    // The canonical EIP-8141 expiry verifier, as it appears in genesis.
    final Address expiryVerifier = MainnetTransactionValidator.EXPIRY_VERIFIER;
    final Bytes expiryVerifierCode =
        Bytes.fromHexString("0x60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd");
    final WorldUpdater setup = worldState.updater();
    final MutableAccount verifier = setup.createAccount(expiryVerifier, 1, Wei.ZERO);
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
  void malformedSignatureEntryInvalidatesTheTransaction() {
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
        .isEqualTo(TransactionInvalidReason.INVALID_TRANSACTION_FORMAT);
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

  // EIP-7906 POST_TX frames.

  private static final Address ASSERTION =
      Address.fromHexString("0x5000000000000000000000000000000000000005");
  private static final Address SECOND_ASSERTION =
      Address.fromHexString("0x6000000000000000000000000000000000000006");
  // SSTORE(0, 1); LOG0(0, 0); STOP
  private static final Bytes WRITE_AND_LOG_CODE = Bytes.fromHexString("0x60015f555f5fa000");
  private static final Bytes STOP_CODE = Bytes.fromHexString("0x00");
  private static final Bytes INVALID_CODE = Bytes.fromHexString("0xfe");

  private void deploy(final Address address, final Bytes code) {
    final WorldUpdater setup = worldState.updater();
    setup.getOrCreate(address).setCode(code);
    setup.commit();
  }

  private static Frame postTxFrame(final Address target) {
    return new Frame(
        Frame.MODE_POST_TX, 0, Optional.of(target), EXEC_LIMIT, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  private static Frame bodyFrame(final int mode, final int flags, final Address target) {
    return new Frame(
        mode, flags, Optional.of(target), EXEC_LIMIT, STATE_LIMIT, Wei.ZERO, Bytes.EMPTY);
  }

  /** The body of the POST_TX tests: a 1000 wei transfer to a new account and a write with a log. */
  private List<Frame> bodyThen(final Frame... postTx) {
    deploy(contractAddress, WRITE_AND_LOG_CODE);
    final List<Frame> frames = new java.util.ArrayList<>();
    frames.add(selfVerifyFrame());
    frames.add(senderFrame(recipient, Wei.of(1000)));
    frames.add(bodyFrame(Frame.MODE_SENDER, 0, contractAddress));
    frames.addAll(List.of(postTx));
    return frames;
  }

  private static List<Integer> statuses(final TransactionProcessingResult result) {
    return result.getFrameTransactionOutcome().orElseThrow().frameReceipts().stream()
        .map(FrameReceipt::status)
        .toList();
  }

  private static FrameReceipt receipt(final TransactionProcessingResult result, final int frame) {
    return result.getFrameTransactionOutcome().orElseThrow().frameReceipts().get(frame);
  }

  /** The gas the payer was charged for, from the receipts, as the EIP-8141 settlement takes it. */
  private static long expectedGasUsed(
      final Transaction transaction, final TransactionProcessingResult result) {
    final List<FrameReceipt> receipts =
        result.getFrameTransactionOutcome().orElseThrow().frameReceipts();
    final long execution = receipts.stream().mapToLong(FrameReceipt::executionGasUsed).sum();
    final long state = receipts.stream().mapToLong(FrameReceipt::stateGasUsed).sum();
    return Math.max(
            org.hyperledger.besu.ethereum.core.FrameTransactionGas.intrinsicGas(transaction)
                + execution,
            org.hyperledger.besu.ethereum.core.FrameTransactionGas.calldataFloorGas(transaction))
        + state;
  }

  @Test
  void passingPostTxFramesKeepTheBody() {
    deploy(ASSERTION, STOP_CODE);
    final Transaction transaction =
        signedFrameTransaction(0, bodyThen(postTxFrame(ASSERTION), postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(statuses(result)).containsExactly(1, 1, 1, 1, 1);
    // a cold entry, then a warm one: a passing POST_TX frame keeps its warmth for the next
    assertThat(receipt(result, 3).executionGasUsed()).isEqualTo(3000L);
    assertThat(receipt(result, 4).executionGasUsed()).isEqualTo(100L);
    assertThat(receipt(result, 3).stateGasUsed()).isZero();
    assertThat(worldState.get(recipient).getBalance()).isEqualTo(Wei.of(1000));
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ONE);
    // the EIP-7708 transfer log and the LOG0
    assertThat(result.getLogs()).hasSize(2);
    assertThat(result.getStateGasUsed()).isGreaterThan(0L);
  }

  @Test
  void aRevertingPostTxFrameRevertsTheBodyAndSkipsTheRest() {
    deploy(ASSERTION, REVERTING_CODE);
    deploy(SECOND_ASSERTION, STOP_CODE);
    final Transaction transaction =
        signedFrameTransaction(0, bodyThen(postTxFrame(ASSERTION), postTxFrame(SECOND_ASSERTION)));
    final Wei senderBalanceBefore = worldState.get(sender).getBalance();

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(statuses(result)).containsExactly(1, 1, 1, 0, 2);
    // the body keeps its status and execution gas, and loses its logs and state gas
    assertThat(receipt(result, 1).executionGasUsed()).isEqualTo(3000L);
    assertThat(receipt(result, 1).logs()).isEmpty();
    assertThat(receipt(result, 1).stateGasUsed()).isZero();
    assertThat(receipt(result, 2).logs()).isEmpty();
    assertThat(receipt(result, 2).stateGasUsed()).isZero();
    // a cold entry, PUSH0, PUSH0 and a REVERT that keeps the rest of the budget
    assertThat(receipt(result, 3).executionGasUsed()).isEqualTo(3004L);
    assertThat(receipt(result, 4).executionGasUsed()).isZero();
    assertThat(receipt(result, 4).stateGasUsed()).isZero();
    assertThat(result.getLogs()).isEmpty();
    assertThat(result.getStateGasUsed()).isZero();
    // the body's transfer and write are gone; the prefix's nonce and the payment stay
    assertThat(worldState.get(recipient)).isNull();
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
    assertThat(worldState.get(sender).getNonce()).isEqualTo(1L);
    final long gasUsed = result.getEstimateGasUsedByTransaction();
    assertThat(gasUsed).isEqualTo(expectedGasUsed(transaction, result));
    assertThat(worldState.get(sender).getBalance())
        .isEqualTo(senderBalanceBefore.subtract(BASE_FEE.add(PRIORITY_FEE).multiply(gasUsed)));
  }

  @Test
  void anExceptionalHaltInAPostTxFrameUsesItsWholeBudget() {
    deploy(ASSERTION, INVALID_CODE);
    final Transaction transaction = signedFrameTransaction(0, bodyThen(postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(statuses(result)).containsExactly(1, 1, 1, 0);
    assertThat(receipt(result, 3).executionGasUsed()).isEqualTo(EXEC_LIMIT);
    assertThat(worldState.get(recipient)).isNull();
  }

  @Test
  void anEarlierPassingPostTxFrameKeepsItsReceipt() {
    deploy(ASSERTION, STOP_CODE);
    deploy(SECOND_ASSERTION, REVERTING_CODE);
    final Transaction transaction =
        signedFrameTransaction(
            0,
            bodyThen(
                postTxFrame(ASSERTION), postTxFrame(SECOND_ASSERTION), postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 1, 0, 2);
    assertThat(receipt(result, 3).executionGasUsed()).isEqualTo(3000L);
    assertThat(worldState.get(recipient)).isNull();
  }

  @Test
  void aPostTxFrameWithoutAPayerLeavesTheTransactionInvalid() {
    deploy(ASSERTION, STOP_CODE);
    final TransactionProcessingResult result =
        process(signedFrameTransaction(0, List.of(postTxFrame(ASSERTION))));

    assertThat(result.isInvalid()).isTrue();
    assertThat(result.getValidationResult().getInvalidReason())
        .isEqualTo(TransactionInvalidReason.FRAME_PAYMENT_NOT_APPROVED);
    assertThat(result.getValidationResult().getErrorMessage())
        .isEqualTo("no frame approved payment for the transaction");
  }

  @Test
  void aFailingPostTxFrameRevertsACommittedBodyBatch() {
    deploy(ASSERTION, REVERTING_CODE);
    deploy(contractAddress, WRITE_AND_LOG_CODE);
    final Transaction transaction =
        signedFrameTransaction(
            0,
            List.of(
                selfVerifyFrame(),
                bodyFrame(Frame.MODE_SENDER, Frame.ATOMIC_BATCH_FLAG, contractAddress),
                senderFrame(recipient, Wei.of(1000)),
                postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 0);
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
    assertThat(worldState.get(recipient)).isNull();
  }

  @Test
  void aPostTxFrameObservesAnUnrolledBodyBatch() {
    deploy(secondContractAddress, REVERTING_CODE);
    // reverts unless BALANCE(recipient) == 0
    deploy(
        ASSERTION,
        Bytes.fromHexString(
            "0x73" + recipient.getBytes().toUnprefixedHexString() + "3115601d575f5ffd5b00"));
    final Transaction transaction =
        signedFrameTransaction(
            0,
            List.of(
                selfVerifyFrame(),
                new Frame(
                    Frame.MODE_SENDER,
                    Frame.ATOMIC_BATCH_FLAG,
                    Optional.of(recipient),
                    EXEC_LIMIT,
                    STATE_LIMIT,
                    Wei.of(1000),
                    Bytes.EMPTY),
                bodyFrame(Frame.MODE_DEFAULT, 0, secondContractAddress),
                postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(statuses(result)).containsExactly(1, 1, 0, 1);
    assertThat(receipt(result, 1).logs()).isEmpty();
    assertThat(worldState.get(recipient)).isNull();
  }

  @Test
  void aPostTxFrameIsStatic() {
    deploy(ASSERTION, STORE_ONE_CODE);
    final Transaction transaction = signedFrameTransaction(0, bodyThen(postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 0);
    assertThat(receipt(result, 3).executionGasUsed()).isEqualTo(EXEC_LIMIT);
    assertThat(worldState.get(ASSERTION).getStorageValue(UInt256.ZERO)).isEqualTo(UInt256.ZERO);
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ZERO);
  }

  @Test
  void approveHaltsInAPostTxFrameEvenWithApprovalFlags() {
    // a sponsor whose APPROVE(PAYMENT) would pay, targeted by a POST_TX frame allowed to approve
    final WorldUpdater setup = worldState.updater();
    setup.createAccount(ASSERTION, 0, Wei.of(10).pow(18)).setCode(PAYMASTER_CODE);
    setup.commit();
    final Frame approvingPostTx =
        new Frame(
            Frame.MODE_POST_TX,
            Frame.FLAG_APPROVE_PAYMENT,
            Optional.of(ASSERTION),
            EXEC_LIMIT,
            0L,
            Wei.ZERO,
            Bytes.EMPTY);
    final Transaction transaction = signedFrameTransaction(0, bodyThen(approvingPostTx));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 0);
    assertThat(receipt(result, 3).executionGasUsed()).isEqualTo(EXEC_LIMIT);
    assertThat(result.getFrameTransactionOutcome().orElseThrow().payer()).isEqualTo(sender);
    assertThat(worldState.get(ASSERTION).getBalance()).isEqualTo(Wei.of(10).pow(18));
  }

  @Test
  void approveHaltsInACallFromAPostTxFrame() {
    deploy(secondContractAddress, PAYMASTER_CODE);
    // CALL(50000, second contract, 0, 0, 0, 0, 0); reverts unless the call failed; STOP
    deploy(
        ASSERTION,
        Bytes.fromHexString(
            "0x5f5f5f5f5f73"
                + secondContractAddress.getBytes().toUnprefixedHexString()
                + "61c350f1156025575f5ffd5b00"));
    final Transaction transaction = signedFrameTransaction(0, bodyThen(postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 1);
    // the callee halted and used all 50000 it was given
    assertThat(receipt(result, 3).executionGasUsed()).isGreaterThan(53_000L);
  }

  @Test
  void aPostTxFrameIsCalledFromTheEntryPoint() {
    // reverts unless CALLER == ORIGIN == 0xaa
    deploy(ASSERTION, Bytes.fromHexString("0x3360aa143260aa1416600f575f5ffd5b00"));
    final Transaction transaction = signedFrameTransaction(0, bodyThen(postTxFrame(ASSERTION)));

    assertThat(statuses(process(transaction))).containsExactly(1, 1, 1, 1);
  }

  @Test
  void aCodelessPostTxTargetSucceeds() {
    final Transaction transaction =
        signedFrameTransaction(0, bodyThen(postTxFrame(secondContractAddress)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 1);
    assertThat(receipt(result, 3).executionGasUsed()).isEqualTo(3000L);
  }

  @Test
  void aFailingPostTxFrameRestoresThePrefixStateCharge() {
    // toggles slot 0: sets it to 1 when zero, clears it when set
    deploy(contractAddress, Bytes.fromHexString("0x5f54600114600d5760015f55005b5f5f5500"));
    deploy(ASSERTION, REVERTING_CODE);
    final Frame toggle = bodyFrame(Frame.MODE_DEFAULT, 0, contractAddress);
    final Transaction transaction =
        signedFrameTransaction(
            0, List.of(toggle, selfVerifyFrame(), toggle, postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 0);
    // the body's clear refilled the prefix frame's charge; the revert restores it
    assertThat(receipt(result, 0).stateGasUsed()).isGreaterThan(0L);
    assertThat(receipt(result, 2).stateGasUsed()).isZero();
    assertThat(result.getStateGasUsed()).isEqualTo(receipt(result, 0).stateGasUsed());
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ONE);
    assertThat(result.getEstimateGasUsedByTransaction())
        .isEqualTo(expectedGasUsed(transaction, result));
  }

  /**
   * The TXTRACE part of the bogota-devnet-1 block 1792 probe: fifteen {@code TXTRACE(p, 0); POP}
   * reads after a 1 wei transfer, every one of which must find its entry (two balance changes, and
   * the transfer's EIP-7708 log with three topics). Each read costs 3 + 3 + 100 + 2 after the cold
   * entry.
   */
  @Test
  void theLiveTxtraceProbeReadsEveryTableEntry() {
    final StringBuilder probe = new StringBuilder("0x");
    for (final int param :
        List.of(
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x13, 0x14,
            0x15)) {
      probe.append(String.format("600060%02xb650", param));
    }
    deploy(ASSERTION, Bytes.fromHexString(probe.append("00").toString()));
    final Address dead = Address.fromHexString("0xdead");
    final Transaction transaction =
        signedFrameTransaction(
            0, List.of(selfVerifyFrame(), senderFrame(dead, Wei.ONE), postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(statuses(result)).containsExactly(1, 1, 1);
    assertThat(receipt(result, 2).executionGasUsed()).isEqualTo(3000L + 15L * 108L);
    assertThat(worldState.get(dead).getBalance()).isEqualTo(Wei.ONE);
  }

  @Test
  void aPostTxFrameReadsTheTransactionDiff() {
    deploy(contractAddress, WRITE_AND_LOG_CODE);
    // reverts unless TXTRACE(SLOTS_CHANGED) == 1, TXTRACE(BALANCES_CHANGED) == 2 (the sender's
    // escrow and the recipient), TXTRACE(EVENTS_COUNT) == 2, and TXTRACE(GAS_PAYER) is the sender
    final String check =
        "5f6001b6600114"
            + "5f5fb6600214"
            + "16"
            + "5f600cb6600214"
            + "16"
            + "5f6015b673"
            + sender.getBytes().toUnprefixedHexString()
            + "14"
            + "16";
    final int dest = check.length() / 2 + 6;
    deploy(
        ASSERTION, Bytes.fromHexString("0x" + check + String.format("60%02x575f5ffd5b00", dest)));
    final Transaction transaction =
        signedFrameTransaction(
            0,
            List.of(
                selfVerifyFrame(),
                senderFrame(recipient, Wei.of(1000)),
                bodyFrame(Frame.MODE_SENDER, 0, contractAddress),
                postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 1);
    assertThat(worldState.get(contractAddress).getStorageValue(UInt256.ZERO))
        .isEqualTo(UInt256.ONE);
  }

  /**
   * The block access list is built from the transaction updater, so computing the diff, which only
   * reads, must not change it: a POST_TX frame that reads the diff leaves the same partial view as
   * one that does not.
   */
  @Test
  void readingTheDiffRecordsNothingInTheBlockAccessList() {
    // TXTRACE(BALANCES_CHANGED, 0); POP; STOP, against code that burns the same 106 gas without
    // reading the diff, so the payer and the coinbase settle the same amounts
    final Optional<PartialBlockAccessView> reading =
        accessViewWithPostTxCode(Bytes.fromHexString("0x5f5fb65000"));
    setUp();
    final Optional<PartialBlockAccessView> notReading =
        accessViewWithPostTxCode(Bytes.fromHexString("0x5f5f5050" + "5f50".repeat(24) + "5b5b00"));

    assertThat(reading).isPresent();
    assertThat(notReading).isPresent();
    // the account change entries have no value equality, so the views are compared as text
    assertThat(reading.get().toString()).isEqualTo(notReading.get().toString());
  }

  private Optional<PartialBlockAccessView> accessViewWithPostTxCode(final Bytes postTxCode) {
    deploy(ASSERTION, postTxCode);
    final Transaction transaction = signedFrameTransaction(0, bodyThen(postTxFrame(ASSERTION)));
    // the tracker reads the transaction updater, a stacked updater as in block processing
    final WorldUpdater transactionUpdater = worldState.updater().updater();
    final TransactionProcessingResult result =
        processor()
            .processTransaction(
                transactionUpdater,
                blockHeader,
                transaction,
                coinbase,
                OperationTracer.NO_TRACING,
                blockHashLookup,
                TransactionValidationParams.processingBlock(),
                Wei.ZERO,
                Optional.of(new AccessLocationTracker(1, false)));
    assertThat(result.isInvalid() ? result.getValidationResult().getErrorMessage() : "").isEmpty();
    assertThat(statuses(result)).containsExactly(1, 1, 1, 1);
    return result.getPartialBlockAccessView();
  }

  // EIP-7906 TXDIFF.

  private static final Address DEAD = Address.fromHexString("0xdead");

  /**
   * The bogota-devnet-1 probe at 0x207cf3dedd5227fed11517f15fb6646582eb971b, 100 bytes: fifteen
   * {@code TXTRACE(p, 0); POP}, then {@code TXDIFF(CODEHASH_BEFORE, 0xdead, 0); POP; STOP}.
   */
  private static final Bytes LIVE_PROBE_CODE = liveProbeCode();

  private static Bytes liveProbeCode() {
    final StringBuilder probe = new StringBuilder("0x");
    for (final int param :
        List.of(
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x13, 0x14,
            0x15)) {
      probe.append(String.format("600060%02xb650", param));
    }
    return Bytes.fromHexString(probe.append("600061dead6004b75000").toString());
  }

  /** PUSH1 in3, PUSH20 address, PUSH1 param, TXDIFF. */
  private static String txdiffOf(final int param, final Address address, final int in3) {
    return String.format(
        "60%02x73%s60%02xb7", in3, address.getBytes().toUnprefixedHexString(), param);
  }

  /** Code that runs the checks, each leaving 1 when it holds, and reverts unless all of them do. */
  private static Bytes assertingAll(final String... checks) {
    final StringBuilder code = new StringBuilder(checks[0]);
    for (int i = 1; i < checks.length; i++) {
      // AND
      code.append(checks[i]).append("16");
    }
    // PUSH2 dest; JUMPI; PUSH0; PUSH0; REVERT; JUMPDEST; STOP
    final int dest = code.length() / 2 + 7;
    return Bytes.fromHexString("0x" + code + String.format("61%04x575f5ffd5b00", dest));
  }

  /**
   * Block 1792 of bogota-devnet-1 (tx 0x5b3e5507…): its POST_TX frame used 4731 gas on chain, the
   * cold entry, fifteen TXTRACE reads at 108 each, and 111 for TXDIFF(CODEHASH_BEFORE, 0xdead) with
   * its pushes and POP, 0xdead being warm from the SENDER frame's entry.
   */
  @Test
  void theLiveBlock1792ProbeUses4731Gas() {
    deploy(ASSERTION, LIVE_PROBE_CODE);
    final Transaction transaction =
        signedFrameTransaction(
            0, List.of(selfVerifyFrame(), senderFrame(DEAD, Wei.ONE), postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(LIVE_PROBE_CODE.size()).isEqualTo(100);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(statuses(result)).containsExactly(1, 1, 1);
    assertThat(receipt(result, 1).executionGasUsed()).isEqualTo(3000L);
    assertThat(receipt(result, 2).executionGasUsed()).isEqualTo(4731L);
    assertThat(worldState.get(DEAD).getBalance()).isEqualTo(Wei.ONE);
    // the EIP-7708 transfer log
    assertThat(result.getLogs()).hasSize(1);
  }

  /**
   * Block 1793 of bogota-devnet-1 (tx 0x1efd4dbb…): the first POST_TX frame runs {@code PUSH1 0;
   * PUSH1 0; REVERT} for 3006 gas, which reverts the body, so the SENDER frame keeps its status and
   * 3000 gas but loses its log, and the probe after it is skipped.
   */
  @Test
  void theLiveBlock1793ShapeRevertsTheBody() {
    deploy(ASSERTION, LIVE_PROBE_CODE);
    deploy(SECOND_ASSERTION, Bytes.fromHexString("0x60006000fd"));
    final Transaction transaction =
        signedFrameTransaction(
            0,
            List.of(
                selfVerifyFrame(),
                senderFrame(DEAD, Wei.ONE),
                postTxFrame(SECOND_ASSERTION),
                postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 0, 2);
    assertThat(receipt(result, 1).executionGasUsed()).isEqualTo(3000L);
    assertThat(receipt(result, 1).logs()).isEmpty();
    assertThat(receipt(result, 2).executionGasUsed()).isEqualTo(3006L);
    assertThat(receipt(result, 3).executionGasUsed()).isZero();
    assertThat(result.getLogs()).isEmpty();
    assertThat(worldState.get(DEAD)).isNull();
    assertThat(worldState.get(sender).getNonce()).isEqualTo(1L);
  }

  /**
   * TXDIFF reads "before" values from the transaction prestate and "after" values from the body's
   * result, through the processor's real updater layers. The payer's balance difference includes
   * the pre-charge, which TXTRACE(GAS_PRE_CHARGE) reports.
   */
  @Test
  void aPostTxFrameLooksUpValuesBeforeAndAfterTheBody() {
    deploy(
        ASSERTION,
        assertingAll(
            // contract slot 0: 0 before, 1 after
            txdiffOf(0x00, contractAddress, 0) + "15",
            txdiffOf(0x01, contractAddress, 0) + "600114",
            // the recipient did not exist, and received 1000
            txdiffOf(0x02, recipient, 0) + "15",
            txdiffOf(0x03, recipient, 0) + "6103e814",
            txdiffOf(0x04, recipient, 0)
                + "7f"
                + Hash.EMPTY.getBytes().toUnprefixedHexString()
                + "14",
            // the contract changed only its storage, and emitted one event
            txdiffOf(0x0a, contractAddress, 0) + "600414",
            txdiffOf(0x08, contractAddress, 0) + "600114",
            txdiffOf(0x07, contractAddress, 0) + "15",
            // the sender paid the pre-charge and sent 1000: before - after == pre-charge + 1000
            txdiffOf(0x03, sender, 0)
                + txdiffOf(0x02, sender, 0)
                + "03"
                + "5f6014b6"
                + "6103e8"
                + "01"
                + "14",
            // the sender's nonce and balance changed
            txdiffOf(0x0a, sender, 0) + "600314"));
    final Transaction transaction = signedFrameTransaction(0, bodyThen(postTxFrame(ASSERTION)));

    final TransactionProcessingResult result = process(transaction);

    assertThat(statuses(result)).containsExactly(1, 1, 1, 1);
    assertThat(worldState.get(recipient).getBalance()).isEqualTo(Wei.of(1000));
  }

  /**
   * TXDIFF's state lookups record the slot or the account in the block access list, even for an
   * account that never existed, while its views record nothing.
   */
  @Test
  void txdiffLookupsRecordTheirAccessesInTheBlockAccessList() {
    final Address slotOwner = Address.fromHexString("0x7000000000000000000000000000000000000007");
    final Address account = Address.fromHexString("0x8000000000000000000000000000000000000008");
    final Address viewed = Address.fromHexString("0x9000000000000000000000000000000000000009");
    final Optional<PartialBlockAccessView> view =
        accessViewWithPostTxCode(
            Bytes.fromHexString(
                "0x"
                    + txdiffOf(0x00, slotOwner, 7)
                    + "50"
                    + txdiffOf(0x05, account, 0)
                    + "50"
                    + txdiffOf(0x06, viewed, 0)
                    + "50"
                    + txdiffOf(0x0a, viewed, 0)
                    + "5000"));

    assertThat(view).isPresent();
    final List<PartialBlockAccessView.AccountChanges> changes = view.get().accountChanges();
    final PartialBlockAccessView.AccountChanges slotRead =
        changes.stream().filter(c -> c.getAddress().equals(slotOwner)).findFirst().orElseThrow();
    assertThat(slotRead.getStorageReads())
        .extracting(slot -> slot.getSlotKey().orElseThrow())
        .containsExactly(UInt256.valueOf(7));
    assertThat(slotRead.getStorageChanges()).isEmpty();
    assertThat(slotRead.getPostBalance()).isEmpty();
    final PartialBlockAccessView.AccountChanges accountRead =
        changes.stream().filter(c -> c.getAddress().equals(account)).findFirst().orElseThrow();
    assertThat(accountRead.getStorageReads()).isEmpty();
    assertThat(accountRead.getPostBalance()).isEmpty();
    assertThat(changes).noneMatch(c -> c.getAddress().equals(viewed));
  }
}
