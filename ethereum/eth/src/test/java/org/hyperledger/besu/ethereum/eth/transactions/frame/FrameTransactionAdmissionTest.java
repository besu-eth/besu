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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Admission against real simulation of the validation prefix. {@link
 * FrameTransactionAdmissionEvmV2Test} runs every case again on EVM v2.
 */
class FrameTransactionAdmissionTest {

  private static final SignatureAlgorithm ALGO = SignatureAlgorithmFactory.getInstance();
  private static final BigInteger CHAIN_ID = BigInteger.ONE;
  private static final Wei MAX_FEE = Wei.of(100);
  private static final Wei ONE_ETHER = Wei.of(10).pow(18);
  private static final long VERIFY_LIMIT = 50_000L;

  // APPROVE(offset=0, length=0, scope=APPROVE_PAYMENT)
  private static final String APPROVE_PAYMENT = "60015f5faa";
  // APPROVE(offset=0, length=0, scope=APPROVE_EXECUTION | APPROVE_PAYMENT)
  private static final String APPROVE_BOTH = "600360006000aa";
  // Init code returning APPROVE_BOTH as the runtime code: PUSH7 code; PUSH0; MSTORE; RETURN(25, 7)
  private static final Bytes SMART_ACCOUNT_INIT_CODE =
      Bytes.fromHexString("0x66" + APPROVE_BOTH + "5f52" + "6007" + "6019" + "f3");
  // Copies the calldata to memory, then CREATE2(value 0, memory, salt 0) POP
  private static final String CREATE2_CALLDATA = "365f5f37" + "5f365f5ff550";
  // CREATE(value 0, the calldata already in memory) POP
  private static final String CREATE_CALLDATA = "365f5ff050";
  // EIP-7906 wallet pattern: reverts unless the last frame is a POST_TX frame, that is unless
  // FRAMEPARAM(mode, TXPARAM(frame count) - 1) == 3; then JUMPDEST, where the approval follows.
  private static final String REQUIRE_TRAILING_POST_TX =
      "6002" + "6001" + "6009" + "b0" + "03" + "b3" + "6003" + "14" + "6012" + "57" + "5f5ffd"
          + "5b";

  private final KeyPair senderKeys = ALGO.generateKeyPair();
  private final Address sender = Address.extract(senderKeys.getPublicKey());
  private final Address recipient =
      Address.fromHexString("0x00000000000000000000000000000000000000cc");
  private final Address smartAccount =
      Address.fromHexString("0x00000000000000000000000000000000000000dd");
  private final Address paymaster =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");
  private final Address factory =
      Address.fromHexString("0x00000000000000000000000000000000000000ee");
  private final Address assertion =
      Address.fromHexString("0x00000000000000000000000000000000000000ab");

  ProtocolSchedule protocolSchedule;
  BlockHeader head;
  private MutableWorldState worldState;
  private WorldStateArchive archive;
  private FrameTransactionAdmission admission;

  /**
   * The EVM configuration the protocol schedule is built with.
   *
   * @return the configuration
   */
  EvmConfiguration evmConfiguration() {
    return EvmConfiguration.DEFAULT;
  }

  @BeforeEach
  void setUp() {
    protocolSchedule = bogotaAt(0);
    head =
        new BlockHeaderTestFixture()
            .number(10)
            .timestamp(1_000)
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();
    worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater setup = worldState.updater();
    setup.createAccount(sender, 0, Wei.of(10).multiply(ONE_ETHER));
    setup.commit();

    archive = mock(WorldStateArchive.class);
    when(archive.getWorldState(any())).thenAnswer(invocation -> Optional.of(worldState));
    admission = new FrameTransactionAdmission(protocolSchedule, archive);
  }

  private ProtocolSchedule bogotaAt(final long bogotaTime) {
    return MainnetProtocolSchedule.fromConfig(
        new StubGenesisConfigOptions().bogotaTime(bogotaTime).chainId(CHAIN_ID),
        Optional.of(false),
        Optional.of(evmConfiguration()),
        MiningConfiguration.newDefault(),
        new BadBlockManager(),
        false,
        BalConfiguration.DEFAULT,
        new NoOpMetricsSystem());
  }

  private Transaction build(
      final Address from,
      final long nonce,
      final List<Frame> frames,
      final List<FrameSignature> signatures) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonce(nonce)
        .sender(from)
        .frames(frames)
        .frameSignatures(signatures)
        .maxPriorityFeePerGas(Wei.of(1))
        .maxFeePerGas(MAX_FEE)
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  /** Canonically signs a transaction from the EOA sender with one SECP256K1 entry. */
  private Transaction signed(final long nonce, final List<Frame> frames) {
    final FrameSignature placeholder =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 0, 65));
    final Transaction unsigned = build(sender, nonce, frames, List.of(placeholder));
    final SECPSignature signature =
        ALGO.sign(Bytes32.wrap(unsigned.getFrameSignatureHash().getBytes()), senderKeys);
    final Bytes raw =
        Bytes.concatenate(
            Bytes.of(signature.getRecId()),
            UInt256.valueOf(signature.getR()),
            UInt256.valueOf(signature.getS()));
    return build(
        sender,
        nonce,
        frames,
        List.of(
            new FrameSignature(FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, raw)));
  }

  private static Frame verify(final int flags, final Optional<Address> target, final long gas) {
    return new Frame(Frame.MODE_VERIFY, flags, target, gas, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  private static Frame selfVerify() {
    return verify(Frame.APPROVE_SCOPE_MASK, Optional.empty(), VERIFY_LIMIT);
  }

  private Frame userOp() {
    return new Frame(
        Frame.MODE_SENDER, 0, Optional.of(recipient), 50_000L, 0L, Wei.ONE, Bytes.EMPTY);
  }

  private static Frame deploy(final Address target, final Bytes data, final long gas) {
    return new Frame(Frame.MODE_DEFAULT, 0, Optional.of(target), gas, 400_000L, Wei.ZERO, data);
  }

  /** A self-relayed transfer from the EOA sender, canonically signed. */
  private Transaction selfRelay(final Frame... leading) {
    return selfRelay(0, leading);
  }

  private Transaction selfRelay(final long nonce, final Frame... leading) {
    final List<Frame> frames = new ArrayList<>(List.of(leading));
    frames.add(selfVerify());
    frames.add(userOp());
    return signed(nonce, frames);
  }

  private void deployCode(final Address address, final Bytes code) {
    final WorldUpdater setup = worldState.updater();
    setup.getOrCreate(address).setCode(code);
    setup.commit();
  }

  /**
   * A smart account whose code runs {@code body} and then approves execution and payment, in a
   * self-verify frame followed by {@code after}.
   */
  private Transaction smartAccountRunning(final String body, final Frame... after) {
    final WorldUpdater setup = worldState.updater();
    setup
        .createAccount(smartAccount, 0, Wei.of(10).multiply(ONE_ETHER))
        .setCode(Bytes.fromHexString(body + APPROVE_BOTH));
    setup.commit();
    final List<Frame> frames = new ArrayList<>();
    frames.add(selfVerify());
    frames.addAll(List.of(after));
    return build(smartAccount, 0, frames, List.of());
  }

  /** An EIP-7906 POST_TX frame asserting with the code at {@code target}. */
  private static Frame postTx(final Address target) {
    return new Frame(
        Frame.MODE_POST_TX, 0, Optional.of(target), 50_000L, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  /**
   * A transaction from the counterfactual smart account that {@code factoryCode} deploys with
   * CREATE2: a deploy frame calling the factory with the init code, then a self-verify frame.
   */
  private Transaction counterfactualDeploy(final String factoryCode) {
    deployCode(factory, Bytes.fromHexString(factoryCode));
    final Address account =
        Address.extract(
            Bytes32.wrap(
                Hash.hash(
                        Bytes.concatenate(
                            Bytes.of(0xff),
                            factory.getBytes(),
                            Bytes32.ZERO,
                            Hash.hash(SMART_ACCOUNT_INIT_CODE).getBytes()))
                    .getBytes()));
    final WorldUpdater setup = worldState.updater();
    setup.createAccount(account, 0, Wei.of(10).multiply(ONE_ETHER));
    setup.commit();
    return build(
        account,
        0,
        List.of(
            deploy(factory, SMART_ACCOUNT_INIT_CODE, 80_000L),
            verify(Frame.APPROVE_SCOPE_MASK, Optional.empty(), 20_000L)),
        List.of());
  }

  private static void assertPolicyViolation(
      final FrameTransactionAdmission.Result result, final String message) {
    assertThat(result.isAdmitted()).isFalse();
    assertThat(result.rejection())
        .as(String.valueOf(result.message()))
        .isEqualTo(TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION);
    assertThat(result.message()).contains(message);
  }

  @Test
  void admitsASignedSelfRelay() {
    final Transaction transaction = selfRelay();
    final FrameTransactionAdmission.Result result = admission.admit(transaction, head);

    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
    final FrameTransactionAdmission.Admitted admitted = result.admitted().get();
    assertThat(admitted.payer()).isEqualTo(sender);
    assertThat(admitted.codedPaymaster()).isFalse();
    assertThat(admitted.payerBalance()).isEqualTo(Wei.of(10).multiply(ONE_ETHER));
    assertThat(admitted.maxCost()).isEqualTo(MAX_FEE.multiply(Wei.of(transaction.getGasLimit())));
    // Simulation must not leak into the head state.
    assertThat(worldState.get(sender).getNonce()).isZero();
    assertThat(worldState.get(recipient)).isNull();
  }

  @Test
  void admitsAPaymasterPrefixAndFlagsTheCodedPaymaster() {
    final WorldUpdater setup = worldState.updater();
    setup.createAccount(paymaster, 0, ONE_ETHER).setCode(Bytes.fromHexString(APPROVE_PAYMENT));
    setup.commit();
    final Transaction transaction =
        signed(
            0,
            List.of(
                verify(Frame.FLAG_APPROVE_EXECUTION, Optional.empty(), 30_000L),
                verify(Frame.FLAG_APPROVE_PAYMENT, Optional.of(paymaster), 30_000L),
                userOp()));

    final FrameTransactionAdmission.Result result = admission.admit(transaction, head);

    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
    final FrameTransactionAdmission.Admitted admitted = result.admitted().get();
    assertThat(admitted.prefix().paymaster()).isTrue();
    assertThat(admitted.prefix().payFrame()).isEqualTo(1);
    assertThat(admitted.payer()).isEqualTo(paymaster);
    assertThat(admitted.payerBalance()).isEqualTo(ONE_ETHER);
    assertThat(admitted.codedPaymaster()).isTrue();
  }

  @Test
  void admitsSmartAccountCodeThatReadsOnlyItsOwnStorage() {
    final FrameTransactionAdmission.Result result =
        admission.admit(smartAccountRunning("600054" + "50"), head); // SLOAD(0) POP
    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
  }

  @Test
  void rejectsBannedOpcodesInTheValidationPrefix() {
    assertPolicyViolation(
        admission.admit(smartAccountRunning("4750"), head), // SELFBALANCE POP
        "SELFBALANCE is banned");
  }

  @Test
  void rejectsTimestampOutsideTheCanonicalExpiryVerifier() {
    assertPolicyViolation(
        admission.admit(smartAccountRunning("4250"), head), // TIMESTAMP POP
        "TIMESTAMP outside the canonical expiry verifier");
  }

  @Test
  void admitsTheCanonicalExpiryVerifier() {
    deployCode(
        MainnetTransactionValidator.EXPIRY_VERIFIER, ValidationTraceRules.EXPIRY_VERIFIER_CODE);

    final FrameTransactionAdmission.Result result =
        admission.admit(selfRelay(ValidationPrefixTest.expiry(head.getTimestamp() + 100)), head);

    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
    assertThat(result.admitted().get().prefix().expiryDeadline())
        .hasValue(head.getTimestamp() + 100);
    assertThat(result.admitted().get().prefix().payFrame()).isEqualTo(1);
  }

  @Test
  void rejectsTimestampInOtherCodeAtTheExpiryVerifierAddress() {
    deployCode(MainnetTransactionValidator.EXPIRY_VERIFIER, Bytes.fromHexString("0x425000"));

    assertPolicyViolation(
        admission.admit(selfRelay(ValidationPrefixTest.expiry(head.getTimestamp() + 100)), head),
        "frame 0: TIMESTAMP outside the canonical expiry verifier");
  }

  @Test
  void rejectsGasNotImmediatelyFollowedByACall() {
    assertPolicyViolation(
        admission.admit(smartAccountRunning("5a50"), head), // GAS POP
        "GAS not immediately followed by a call");
  }

  @Test
  void admitsGasImmediatelyFollowedByAValuelessCallToAPrecompile() {
    // CALL(GAS, identity, value 0, 0, 0, 0, 0) POP
    final FrameTransactionAdmission.Result result =
        admission.admit(smartAccountRunning("5f5f5f5f5f" + "6004" + "5af1" + "50"), head);
    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
  }

  @Test
  void rejectsAValueBearingCall() {
    // CALL(GAS, identity, value 1, 0, 0, 0, 0) POP
    assertPolicyViolation(
        admission.admit(smartAccountRunning("5f5f5f5f" + "6001" + "6004" + "5af1" + "50"), head),
        "CALL transfers value");
  }

  @Test
  void rejectsReadsOfAnotherAccountsState() {
    // EXTCODESIZE of an account with no code.
    assertPolicyViolation(
        admission.admit(
            smartAccountRunning("73" + recipient.getBytes().toUnprefixedHexString() + "3b50"),
            head),
        "has no code");
  }

  @Test
  void rejectsCreateOutsideTheDeployFrame() {
    assertPolicyViolation(
        admission.admit(smartAccountRunning("5f5f5ff050"), head), // CREATE(0, 0, 0) POP
        "CREATE outside the deploy frame");
  }

  @Test
  void admitsADeployFrameThatInstallsTheSendersCode() {
    final Transaction transaction = counterfactualDeploy(CREATE2_CALLDATA + "00");

    final FrameTransactionAdmission.Result result = admission.admit(transaction, head);

    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
    assertThat(result.admitted().get().prefix().deployFrame()).isZero();
    assertThat(result.admitted().get().payer()).isEqualTo(transaction.getSender());
    // The deployment only happened in the simulation.
    assertThat(worldState.get(transaction.getSender()).getCode().isEmpty()).isTrue();
  }

  @Test
  void rejectsADeployFrameThatAlsoCreatesCodeOffTheSender() {
    // The sender's code is deployed and approves, but the factory deploys a second contract.
    assertPolicyViolation(
        admission.admit(counterfactualDeploy(CREATE2_CALLDATA + CREATE_CALLDATA + "00"), head),
        "frame 0: deploy frame created code at ");
  }

  @Test
  void rejectsADeployFrameThatInstallsNoCodeAtTheSender() {
    deployCode(factory, Bytes.fromHexString("0x00"));
    assertPolicyViolation(
        admission.admit(selfRelay(deploy(factory, Bytes.EMPTY, 20_000L)), head),
        "deploy frame installed no code at the sender");
  }

  @Test
  void rejectsAPrefixThatDoesNotApprove() {
    final FrameTransactionAdmission.Result result =
        admission.admit(smartAccountRunning("60006000fd"), head); // REVERT
    assertThat(result.isAdmitted()).isFalse();
    assertThat(result.rejection()).isEqualTo(TransactionInvalidReason.FRAME_VERIFICATION_FAILED);
  }

  @Test
  void rejectsAFutureNonce() {
    // The transaction pool accepts a future nonce for the other types; a frame transaction must
    // be runnable at the next block.
    final FrameTransactionAdmission.Result result = admission.admit(selfRelay(1), head);
    assertThat(result.isAdmitted()).isFalse();
    assertThat(result.rejection()).isEqualTo(TransactionInvalidReason.NONCE_TOO_HIGH);
  }

  @Test
  void rejectsUnrecognizedShapesWithoutSimulating() {
    final Transaction transaction = build(sender, 0, List.of(userOp()), List.of());
    assertPolicyViolation(admission.admit(transaction, head), "no recognized shape");
  }

  @Test
  void rejectsAnExpiredDeadline() {
    assertPolicyViolation(
        admission.admit(selfRelay(ValidationPrefixTest.expiry(head.getTimestamp() - 1)), head),
        "expiry");
  }

  @Test
  void rejectsABlobCarryingFrameTransaction() {
    final Transaction transaction =
        Transaction.builder()
            .copiedFrom(selfRelay())
            .versionedHashes(List.of(VersionedHash.DEFAULT_VERSIONED_HASH))
            .build();
    assertPolicyViolation(admission.admit(transaction, head), "blob-carrying");
  }

  @Test
  void rejectsWhenFramesAreNotActiveAtTheNextBlock() {
    final FrameTransactionAdmission beforeBogota =
        new FrameTransactionAdmission(bogotaAt(head.getTimestamp() + 2), archive);

    final FrameTransactionAdmission.Result result = beforeBogota.admit(selfRelay(), head);

    assertThat(result.rejection()).isEqualTo(TransactionInvalidReason.INVALID_TRANSACTION_FORMAT);
    assertThat(result.message()).isEqualTo("frame transactions are not active at the next block");
  }

  @Test
  void admitsWhenTheNextBlockActivatesFrames() {
    final FrameTransactionAdmission atActivation =
        new FrameTransactionAdmission(bogotaAt(head.getTimestamp() + 1), archive);

    final FrameTransactionAdmission.Result result = atActivation.admit(selfRelay(), head);

    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
  }

  @Test
  void rejectsWhenTheHeadStateIsNotAvailable() {
    when(archive.getWorldState(any())).thenReturn(Optional.empty());

    final FrameTransactionAdmission.Result result = admission.admit(selfRelay(), head);

    assertThat(result.rejection())
        .isEqualTo(TransactionInvalidReason.CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE);
  }

  @Test
  void admitsATransactionWhosePostTxFrameWouldFail() {
    // geth TestPostTxAdmission: only the validation prefix is simulated, so an assertion that can
    // never pass does not stop admission. A failing POST_TX frame leaves the transaction valid and
    // its payment collected.
    deployCode(assertion, Bytes.fromHexString("0xfe")); // INVALID
    final Transaction transaction = signed(0, List.of(selfVerify(), userOp(), postTx(assertion)));

    final FrameTransactionAdmission.Result result = admission.admit(transaction, head);

    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
    final FrameTransactionAdmission.Admitted admitted = result.admitted().get();
    assertThat(admitted.prefix().length()).isEqualTo(1);
    assertThat(admitted.payer()).isEqualTo(sender);
    // The payer's exposure covers the assertion's budget: max_cost counts every frame.
    assertThat(admitted.maxCost()).isEqualTo(MAX_FEE.multiply(Wei.of(transaction.getGasLimit())));
    assertThat(transaction.getGasLimit())
        .isGreaterThanOrEqualTo(signed(0, List.of(selfVerify(), userOp())).getGasLimit() + 50_000L);
    // Simulation must not leak into the head state.
    assertThat(worldState.get(sender).getNonce()).isZero();
    assertThat(worldState.get(recipient)).isNull();
  }

  @Test
  void rejectsAFrameAfterAPostTxFrameWithTheValidatorsReason() {
    // Classification looks only at the prefix; the simulation validates the whole transaction
    // first, so the EIP-7906 suffix rule rejects the transaction before any frame runs.
    deployCode(assertion, Bytes.fromHexString("0x00"));

    final FrameTransactionAdmission.Result result =
        admission.admit(signed(0, List.of(selfVerify(), postTx(assertion), userOp())), head);

    assertThat(result.isAdmitted()).isFalse();
    assertThat(result.rejection()).isEqualTo(TransactionInvalidReason.INVALID_FRAMES);
    assertThat(result.message()).isEqualTo("frame 2 follows a POST_TX frame");
    assertThat(worldState.get(sender).getNonce()).isZero();
  }

  @Test
  void admitsASmartAccountThatRequiresATrailingPostTxFrame() {
    // EIP-7906 wallet pattern: the account's VERIFY code reads the frame list and approves only
    // with an assertion at the end. TXPARAM and FRAMEPARAM read transaction fields, which the
    // validation trace rules allow.
    deployCode(assertion, Bytes.fromHexString("0x00"));

    final FrameTransactionAdmission.Result withAssertion =
        admission.admit(
            smartAccountRunning(REQUIRE_TRAILING_POST_TX, userOp(), postTx(assertion)), head);
    assertThat(withAssertion.isAdmitted()).as(String.valueOf(withAssertion.message())).isTrue();
    assertThat(withAssertion.admitted().get().payer()).isEqualTo(smartAccount);

    final FrameTransactionAdmission.Result withoutAssertion =
        admission.admit(build(smartAccount, 0, List.of(selfVerify(), userOp()), List.of()), head);
    assertThat(withoutAssertion.isAdmitted()).isFalse();
    assertThat(withoutAssertion.rejection())
        .isEqualTo(TransactionInvalidReason.FRAME_VERIFICATION_FAILED);
  }
}
