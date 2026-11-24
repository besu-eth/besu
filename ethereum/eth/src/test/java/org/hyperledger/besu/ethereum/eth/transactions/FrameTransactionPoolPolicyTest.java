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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.Frame.APPROVE_SCOPE_MASK;
import static org.hyperledger.besu.ethereum.core.Frame.ATOMIC_BATCH_FLAG;
import static org.hyperledger.besu.ethereum.core.Frame.FLAG_APPROVE_EXECUTION;
import static org.hyperledger.besu.ethereum.core.Frame.FLAG_APPROVE_PAYMENT;
import static org.hyperledger.besu.ethereum.core.Frame.MODE_ASSERT;
import static org.hyperledger.besu.ethereum.core.Frame.MODE_DEFAULT;
import static org.hyperledger.besu.ethereum.core.Frame.MODE_SENDER;
import static org.hyperledger.besu.ethereum.core.Frame.MODE_VERIFY;
import static org.hyperledger.besu.ethereum.core.FrameSignature.SCHEME_ARBITRARY;
import static org.hyperledger.besu.ethereum.core.FrameSignature.SCHEME_P256;
import static org.hyperledger.besu.ethereum.core.FrameSignature.SCHEME_SECP256K1;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.fluent.SimpleAccount;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FrameTransactionPoolPolicyTest {

  private static final GasCalculator GAS_CALCULATOR = new AmsterdamGasCalculator();
  private static final Address SENDER =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address OTHER =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  // EIP-8272 recent root contract, EELS RECENT_ROOT_ADDRESS.
  private static final Address RECENT_ROOT_ADDRESS =
      Address.fromHexString("0x8272d9679689ea2f307140cdf9002d27dc00ffff");
  private static final long NONCE = 7L;
  private static final Wei MAX_FEE = Wei.fromEth(1);
  // EIP-8141 MAX_VERIFY_GAS less the secp256k1 signature validation cost.
  private static final long MAX_SELF_VERIFY_EXECUTION_GAS = 297_200L;
  private static final long HEAD_TIMESTAMP = 1_000L;
  private static final long MIN_DEADLINE = HEAD_TIMESTAMP + 24L;
  // The frame entry's cold access to the verifier, 3,000 since EIP-8038, plus the canonical
  // code's 51 gas.
  private static final long MIN_EXPIRY_EXECUTION_GAS = 3_051L;
  private static final Bytes CANONICAL_EXPIRY_VERIFIER_CODE =
      Bytes.fromHexString("0x60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd");
  private static final long NEW_ACCOUNT_STATE_GAS =
      GAS_CALCULATOR.stateGasCostCalculator().newAccountStateGas();

  private static final Frame SELF_VERIFY = selfVerify(Optional.empty(), 5_000L, 0L);
  private static final Frame USER_OP = frame(MODE_SENDER, 0, Optional.of(OTHER), 50_000L);
  private static final Frame POST_OP = frame(MODE_DEFAULT, 0, Optional.of(OTHER), 50_000L);
  private static final Frame CHECK = frame(MODE_ASSERT, 0, Optional.of(OTHER), 50_000L);
  private static final FrameSignature SENDER_SIGNATURE =
      signature(SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY);
  private static final Frame EXPIRY_VERIFY = expiryVerify(5_000L, MIN_DEADLINE);

  static Stream<Arguments> admittedShapes() {
    return Stream.of(
        Arguments.of("self_verify", List.of(SELF_VERIFY)),
        Arguments.of("self_verify, user_op", List.of(SELF_VERIFY, USER_OP)),
        Arguments.of("self_verify, user_op x3", List.of(SELF_VERIFY, USER_OP, USER_OP, USER_OP)),
        Arguments.of("self_verify, post_op, assert", List.of(SELF_VERIFY, POST_OP, CHECK)),
        Arguments.of(
            "batched user_ops after the prefix",
            List.of(
                SELF_VERIFY,
                frame(MODE_SENDER, ATOMIC_BATCH_FLAG, Optional.of(OTHER), 50_000L),
                USER_OP)),
        Arguments.of(
            "explicit sender target", List.of(selfVerify(Optional.of(SENDER), 5_000L, 0L))),
        Arguments.of(
            "warm access execution limit", List.of(selfVerify(Optional.empty(), 100L, 0L))),
        Arguments.of(
            "execution limit at the cap",
            List.of(selfVerify(Optional.empty(), MAX_SELF_VERIFY_EXECUTION_GAS, 0L))),
        Arguments.of(
            "state limit at the cap", List.of(selfVerify(Optional.empty(), 5_000L, 500_000L))),
        Arguments.of("expiry_verify, self_verify", List.of(EXPIRY_VERIFY, SELF_VERIFY)),
        Arguments.of(
            "spamoor default: expiry_verify, self_verify, user_op",
            List.of(EXPIRY_VERIFY, SELF_VERIFY, USER_OP)),
        Arguments.of(
            "expiry deadline 2^64 - 1", List.of(expiryVerify(5_000L, -1L), SELF_VERIFY, USER_OP)),
        Arguments.of(
            "expiry execution limit at the canonical cost",
            List.of(expiryVerify(MIN_EXPIRY_EXECUTION_GAS, MIN_DEADLINE), SELF_VERIFY)));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("admittedShapes")
  void staticCheckAdmitsDirectlyEvaluableShape(final String name, final List<Frame> frames) {
    assertThat(checkStatic(frameTx(frames, List.of(SENDER_SIGNATURE)))).isEqualTo(valid());
  }

  static Stream<Arguments> rejectedShapes() {
    return Stream.of(
        Arguments.of("expiry verifier not first", List.of(SELF_VERIFY, EXPIRY_VERIFY, USER_OP)),
        Arguments.of(
            "two expiry verifier frames", List.of(EXPIRY_VERIFY, EXPIRY_VERIFY, SELF_VERIFY)),
        Arguments.of("expiry verifier alone", List.of(EXPIRY_VERIFY)),
        Arguments.of("expiry verifier before user_op", List.of(EXPIRY_VERIFY, USER_OP)),
        Arguments.of(
            "expiry deadline below the margin",
            List.of(expiryVerify(5_000L, MIN_DEADLINE - 1), SELF_VERIFY)),
        Arguments.of(
            "expiry deadline at the head",
            List.of(expiryVerify(5_000L, HEAD_TIMESTAMP), SELF_VERIFY)),
        Arguments.of(
            "expiry execution limit below the canonical cost",
            List.of(expiryVerify(MIN_EXPIRY_EXECUTION_GAS - 1, MIN_DEADLINE), SELF_VERIFY)),
        Arguments.of(
            "expiry verifier with flags",
            List.of(
                frame(
                    MODE_VERIFY,
                    FLAG_APPROVE_PAYMENT,
                    Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
                    5_000L,
                    Bytes.ofUnsignedLong(MIN_DEADLINE)),
                SELF_VERIFY)),
        Arguments.of(
            "expiry verifier with state gas",
            List.of(
                new Frame(
                    MODE_VERIFY,
                    0,
                    Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
                    5_000L,
                    1L,
                    Wei.ZERO,
                    Bytes.ofUnsignedLong(MIN_DEADLINE)),
                SELF_VERIFY)),
        Arguments.of(
            "expiry verifier with 7 data bytes",
            List.of(
                frame(
                    MODE_VERIFY,
                    0,
                    Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
                    5_000L,
                    Bytes.ofUnsignedLong(MIN_DEADLINE).slice(1)),
                SELF_VERIFY)),
        Arguments.of(
            "recent-root verifier first",
            List.of(frame(MODE_VERIFY, 0, Optional.of(RECENT_ROOT_ADDRESS), 5_000L), SELF_VERIFY)),
        Arguments.of(
            "deploy first",
            List.of(frame(MODE_DEFAULT, 0, Optional.of(OTHER), 50_000L), SELF_VERIFY)),
        Arguments.of(
            "only_verify, pay",
            List.of(
                frame(MODE_VERIFY, FLAG_APPROVE_EXECUTION, Optional.empty(), 5_000L),
                frame(MODE_VERIFY, FLAG_APPROVE_PAYMENT, Optional.of(OTHER), 5_000L))),
        Arguments.of(
            "VERIFY after the prefix",
            List.of(SELF_VERIFY, USER_OP, frame(MODE_VERIFY, 0, Optional.of(OTHER), 5_000L))),
        Arguments.of("user_op first", List.of(USER_OP, SELF_VERIFY)),
        Arguments.of(
            "flags 1", List.of(frame(MODE_VERIFY, FLAG_APPROVE_PAYMENT, Optional.empty(), 5_000L))),
        Arguments.of(
            "flags 2",
            List.of(frame(MODE_VERIFY, FLAG_APPROVE_EXECUTION, Optional.empty(), 5_000L))),
        Arguments.of(
            "flags 7",
            List.of(
                frame(
                    MODE_VERIFY, APPROVE_SCOPE_MASK | ATOMIC_BATCH_FLAG, Optional.empty(), 5_000L),
                USER_OP)),
        Arguments.of(
            "foreign target",
            List.of(frame(MODE_VERIFY, APPROVE_SCOPE_MASK, Optional.of(OTHER), 5_000L))),
        Arguments.of(
            "execution limit above the cap",
            List.of(selfVerify(Optional.empty(), MAX_SELF_VERIFY_EXECUTION_GAS + 1, 0L))),
        Arguments.of("execution limit of 2^64 - 1", List.of(selfVerify(Optional.empty(), -1L, 0L))),
        Arguments.of(
            "execution limit below the warm access",
            List.of(selfVerify(Optional.empty(), 99L, 0L))),
        Arguments.of(
            "state limit of 500_001", List.of(selfVerify(Optional.empty(), 5_000L, 500_001L))),
        Arguments.of(
            "state limit of 2^64 - 1", List.of(selfVerify(Optional.empty(), 5_000L, -1L))));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("rejectedShapes")
  void staticCheckRejectsOtherShapes(final String name, final List<Frame> frames) {
    assertViolation(checkStatic(frameTx(frames, List.of(SENDER_SIGNATURE))));
  }

  // The expiry and self_verify execution limits plus the signature's 2,800 must not exceed
  // MAX_VERIFY_GAS, 300,000.
  static Stream<Arguments> prefixExecutionLimits() {
    return Stream.of(
        Arguments.of("expiry 5,000, self_verify 5,000", 5_000L, 5_000L, true),
        Arguments.of("expiry 5,000, self_verify 292,200: at the cap", 5_000L, 292_200L, true),
        Arguments.of("expiry 5,000, self_verify 292,201: 1 above", 5_000L, 292_201L, false),
        Arguments.of("expiry 292,200, self_verify 5,000: at the cap", 292_200L, 5_000L, true),
        Arguments.of("expiry 292,201, self_verify 5,000: 1 above", 292_201L, 5_000L, false),
        Arguments.of(
            "expiry 3,051, self_verify 294,149: at the cap",
            MIN_EXPIRY_EXECUTION_GAS,
            294_149L,
            true),
        Arguments.of(
            "expiry 3,051, self_verify 294,150: 1 above",
            MIN_EXPIRY_EXECUTION_GAS,
            294_150L,
            false),
        Arguments.of("expiry 2^64 - 1", -1L, 5_000L, false),
        Arguments.of("self_verify 2^64 - 1", 5_000L, -1L, false),
        Arguments.of("both 2^64 - 1", -1L, -1L, false),
        Arguments.of("limits whose 64-bit sum wraps to 1,000", -4_000L, 5_000L, false));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("prefixExecutionLimits")
  void staticCheckBoundsPrefixExecutionLimitsPlusSignatureByMaxVerifyGas(
      final String name,
      final long expiryExecutionGas,
      final long selfVerifyExecutionGas,
      final boolean admitted) {
    final List<Frame> frames =
        List.of(
            expiryVerify(expiryExecutionGas, MIN_DEADLINE),
            selfVerify(Optional.empty(), selfVerifyExecutionGas, 0L));
    assertAdmitted(checkStatic(frameTx(frames, List.of(SENDER_SIGNATURE))), admitted);
  }

  static Stream<Arguments> rejectedSignatures() {
    final Bytes digest = Bytes.repeat((byte) 0x11, 32);
    return Stream.of(
        Arguments.of("no signature", List.of()),
        Arguments.of("two signatures", List.of(SENDER_SIGNATURE, SENDER_SIGNATURE)),
        Arguments.of("P256", List.of(signature(SCHEME_P256, Bytes.EMPTY, Bytes.EMPTY))),
        Arguments.of("ARBITRARY", List.of(signature(SCHEME_ARBITRARY, Bytes.EMPTY, Bytes.EMPTY))),
        Arguments.of(
            "signer other than the sender",
            List.of(signature(SCHEME_SECP256K1, OTHER.getBytes(), Bytes.EMPTY))),
        Arguments.of(
            "explicit message", List.of(signature(SCHEME_SECP256K1, Bytes.EMPTY, digest))));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("rejectedSignatures")
  void staticCheckRejectsSignaturesTheDefaultCodeCannotUse(
      final String name, final List<FrameSignature> signatures) {
    assertViolation(checkStatic(frameTx(List.of(SELF_VERIFY), signatures)));
  }

  @Test
  void staticCheckAdmitsSignatureNamingTheSender() {
    final Transaction tx =
        frameTx(
            List.of(SELF_VERIFY),
            List.of(signature(SCHEME_SECP256K1, SENDER.getBytes(), Bytes.EMPTY)));
    assertThat(checkStatic(tx)).isEqualTo(valid());
  }

  static Stream<Arguments> rejectedTransactions() {
    return Stream.of(
        Arguments.of(
            "keyed nonce",
            frameTxBuilder(List.of(SELF_VERIFY), List.of(SENDER_SIGNATURE))
                .nonceKeys(List.of(UInt256.ONE))
                .build()),
        Arguments.of(
            "blobs",
            frameTxBuilder(List.of(SELF_VERIFY), List.of(SENDER_SIGNATURE))
                .versionedHashes(
                    List.of(new VersionedHash(VersionedHash.SHA256_VERSION_ID, Hash.ZERO)))
                .build()));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("rejectedTransactions")
  void staticCheckRejectsKeyedNoncesAndBlobs(final String name, final Transaction tx) {
    assertViolation(checkStatic(tx));
  }

  @Test
  void staticCheckRejectsSequencedPool() {
    final Transaction tx = frameTx(List.of(SELF_VERIFY), List.of(SENDER_SIGNATURE));
    assertViolation(
        FrameTransactionPoolPolicy.checkStatic(
            tx, Implementation.SEQUENCED, GAS_CALCULATOR, HEAD_TIMESTAMP));
  }

  static Stream<Arguments> expiryVerifierStates() {
    final SimpleAccount canonical =
        new SimpleAccount(MainnetTransactionValidator.EXPIRY_VERIFIER, 1L, Wei.ZERO);
    canonical.setCode(CANONICAL_EXPIRY_VERIFIER_CODE);
    final SimpleAccount modified =
        new SimpleAccount(MainnetTransactionValidator.EXPIRY_VERIFIER, 1L, Wei.ZERO);
    modified.setCode(Bytes.fromHexString("0x00"));
    return Stream.of(
        Arguments.of("canonical code", List.of(EXPIRY_VERIFY, SELF_VERIFY), canonical, true),
        Arguments.of("other code", List.of(EXPIRY_VERIFY, SELF_VERIFY), modified, false),
        Arguments.of("no account", List.of(EXPIRY_VERIFY, SELF_VERIFY), null, false),
        Arguments.of("no expiry verifier frame", List.of(SELF_VERIFY), null, true));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("expiryVerifierStates")
  void expiryVerifierCheckNeedsCanonicalCode(
      final String name, final List<Frame> frames, final Account verifier, final boolean admitted) {
    final WorldView worldView =
        address -> address.equals(MainnetTransactionValidator.EXPIRY_VERIFIER) ? verifier : null;
    assertAdmitted(
        FrameTransactionPoolPolicy.checkExpiryVerifier(
            frameTx(frames, List.of(SENDER_SIGNATURE)), worldView),
        admitted);
  }

  static Stream<Arguments> expiryDeadlines() {
    return Stream.of(
        Arguments.of("expiry verifier first", List.of(EXPIRY_VERIFY, SELF_VERIFY), MIN_DEADLINE),
        Arguments.of("unsigned deadline", List.of(expiryVerify(5_000L, -1L), SELF_VERIFY), -1L),
        Arguments.of("no expiry verifier frame", List.of(SELF_VERIFY), null),
        Arguments.of("expiry verifier not first", List.of(SELF_VERIFY, EXPIRY_VERIFY), null));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("expiryDeadlines")
  void expiryDeadlineReadsTheFirstFrame(
      final String name, final List<Frame> frames, final Long deadline) {
    assertThat(
            FrameTransactionPoolPolicy.expiryDeadline(frameTx(frames, List.of(SENDER_SIGNATURE))))
        .isEqualTo(deadline == null ? OptionalLong.empty() : OptionalLong.of(deadline));
  }

  static Stream<Arguments> senderStates() {
    final SimpleAccount coded = account(NONCE, MAX_FEE);
    coded.setCode(Bytes.fromHexString("0x60006000f3"));
    final SimpleAccount delegated = account(NONCE, MAX_FEE);
    delegated.setCode(
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, OTHER.getBytes()));
    return Stream.of(
        Arguments.of("codeless, balance equal to max_fee", account(NONCE, MAX_FEE), true),
        Arguments.of("codeless, balance above max_fee", account(NONCE, MAX_FEE.add(1)), true),
        Arguments.of("contract code", coded, false),
        Arguments.of("EIP-7702 delegation", delegated, false),
        Arguments.of("nonce gap of 1", account(NONCE - 1, MAX_FEE), false),
        Arguments.of("nonce already used", account(NONCE + 1, MAX_FEE), false),
        Arguments.of("balance of max_fee - 1", account(NONCE, MAX_FEE.subtract(1)), false),
        Arguments.of("no account", null, false));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("senderStates")
  void senderCheckNeedsCodelessFundedSenderAtTheTransactionNonce(
      final String name, final Account senderAccount, final boolean admitted) {
    final Transaction tx = frameTx(List.of(SELF_VERIFY), List.of(SENDER_SIGNATURE));
    final ValidationResult<TransactionInvalidReason> result =
        FrameTransactionPoolPolicy.checkSender(tx, senderAccount, GAS_CALCULATOR);
    assertAdmitted(result, admitted);
  }

  static Stream<Arguments> zeroFeeSenderStates() {
    return Stream.of(
        Arguments.of("no account, state limit 0", null, 0L, 0L, List.of(), false),
        Arguments.of(
            "no account, state limit below new account",
            null,
            0L,
            NEW_ACCOUNT_STATE_GAS - 1,
            List.of(),
            false),
        Arguments.of(
            "no account, state limit covers new account",
            null,
            0L,
            NEW_ACCOUNT_STATE_GAS,
            List.of(),
            true),
        Arguments.of(
            "empty account, state limit 0", account(0L, Wei.ZERO), 0L, 0L, List.of(), false),
        Arguments.of(
            "empty account, state limit covers new account",
            account(0L, Wei.ZERO),
            0L,
            NEW_ACCOUNT_STATE_GAS,
            List.of(),
            true),
        Arguments.of(
            "account with a nonce, state limit 0",
            account(NONCE, Wei.ZERO),
            NONCE,
            0L,
            List.of(),
            true),
        Arguments.of(
            "expiry shape, no account, state limit below new account",
            null,
            0L,
            NEW_ACCOUNT_STATE_GAS - 1,
            List.of(EXPIRY_VERIFY),
            false),
        Arguments.of(
            "expiry shape, no account, state limit covers new account",
            null,
            0L,
            NEW_ACCOUNT_STATE_GAS,
            List.of(EXPIRY_VERIFY),
            true));
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("zeroFeeSenderStates")
  void senderCheckNeedsStateLimitToCreateMissingSender(
      final String name,
      final Account senderAccount,
      final long txNonce,
      final long stateGasLimit,
      final List<Frame> prefix,
      final boolean admitted) {
    final List<Frame> frames =
        Stream.concat(
                prefix.stream(), Stream.of(selfVerify(Optional.empty(), 5_000L, stateGasLimit)))
            .toList();
    final Transaction tx =
        frameTxBuilder(frames, List.of(SENDER_SIGNATURE))
            .nonce(txNonce)
            .maxFee(Wei.ZERO)
            .maxPriorityFeePerGas(Wei.ZERO)
            .build();
    assertAdmitted(
        FrameTransactionPoolPolicy.checkSender(tx, senderAccount, GAS_CALCULATOR), admitted);
  }

  private static void assertAdmitted(
      final ValidationResult<TransactionInvalidReason> result, final boolean admitted) {
    if (admitted) {
      assertThat(result).isEqualTo(valid());
    } else {
      assertViolation(result);
    }
  }

  private static ValidationResult<TransactionInvalidReason> checkStatic(final Transaction tx) {
    return FrameTransactionPoolPolicy.checkStatic(
        tx, Implementation.LAYERED, GAS_CALCULATOR, HEAD_TIMESTAMP);
  }

  private static ValidationResult<TransactionInvalidReason> valid() {
    return ValidationResult.valid();
  }

  private static void assertViolation(final ValidationResult<TransactionInvalidReason> result) {
    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION);
  }

  private static SimpleAccount account(final long nonce, final Wei balance) {
    return new SimpleAccount(SENDER, nonce, balance);
  }

  private static Frame selfVerify(
      final Optional<Address> target, final long executionGasLimit, final long stateGasLimit) {
    return new Frame(
        MODE_VERIFY,
        APPROVE_SCOPE_MASK,
        target,
        executionGasLimit,
        stateGasLimit,
        Wei.ZERO,
        Bytes.EMPTY);
  }

  private static Frame expiryVerify(final long executionGasLimit, final long deadline) {
    return frame(
        MODE_VERIFY,
        0,
        Optional.of(MainnetTransactionValidator.EXPIRY_VERIFIER),
        executionGasLimit,
        Bytes.ofUnsignedLong(deadline));
  }

  private static Frame frame(
      final int mode, final int flags, final Optional<Address> target, final long executionGas) {
    return frame(mode, flags, target, executionGas, Bytes.EMPTY);
  }

  private static Frame frame(
      final int mode,
      final int flags,
      final Optional<Address> target,
      final long executionGas,
      final Bytes data) {
    return new Frame(mode, flags, target, executionGas, 0L, Wei.ZERO, data);
  }

  private static FrameSignature signature(final int scheme, final Bytes signer, final Bytes msg) {
    return new FrameSignature(scheme, signer, msg, Bytes.repeat((byte) 0x01, 65));
  }

  private static Transaction frameTx(
      final List<Frame> frames, final List<FrameSignature> signatures) {
    return frameTxBuilder(frames, signatures).build();
  }

  private static Transaction.Builder frameTxBuilder(
      final List<Frame> frames, final List<FrameSignature> signatures) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.valueOf(1337))
        .sender(SENDER)
        .nonce(NONCE)
        .maxPriorityFeePerGas(Wei.of(500))
        .maxFee(MAX_FEE)
        .frames(frames)
        .frameSignatures(signatures);
  }
}
