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

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
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
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Admission against real simulation of the validation prefix. */
class FrameTransactionAdmissionTest {

  private static final SignatureAlgorithm ALGO = SignatureAlgorithmFactory.getInstance();
  private static final BigInteger CHAIN_ID = BigInteger.ONE;
  private static final Wei MAX_FEE = Wei.of(100);
  private static final long VERIFY_LIMIT = 50_000L;

  private final KeyPair senderKeys = ALGO.generateKeyPair();
  private final Address sender = Address.extract(senderKeys.getPublicKey());
  private final Address recipient =
      Address.fromHexString("0x00000000000000000000000000000000000000cc");
  private final Address smartAccount =
      Address.fromHexString("0x00000000000000000000000000000000000000dd");

  private BlockHeader head;
  private MutableWorldState worldState;
  private FrameTransactionAdmission admission;

  @BeforeEach
  void setUp() {
    final ProtocolSchedule protocolSchedule =
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
    head =
        new BlockHeaderTestFixture()
            .number(10)
            .timestamp(1_000)
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(7))
            .buildHeader();
    worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final WorldUpdater setup = worldState.updater();
    setup.createAccount(sender, 0, Wei.of(10).multiply(Wei.of(10).pow(18)));
    setup.commit();

    final WorldStateArchive archive = mock(WorldStateArchive.class);
    when(archive.getWorldState(any())).thenAnswer(invocation -> Optional.of(worldState));
    admission = new FrameTransactionAdmission(protocolSchedule, archive);
  }

  private Transaction build(
      final Address from, final List<Frame> frames, final List<FrameSignature> signatures) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonce(0)
        .sender(from)
        .frames(frames)
        .frameSignatures(signatures)
        .maxPriorityFeePerGas(Wei.of(1))
        .maxFeePerGas(MAX_FEE)
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  /** A self-relayed transfer from the EOA sender, canonically signed. */
  private Transaction selfRelay(final Frame... leading) {
    final List<Frame> frames = new java.util.ArrayList<>(List.of(leading));
    frames.add(
        new Frame(
            Frame.MODE_VERIFY,
            Frame.APPROVE_SCOPE_MASK,
            Optional.empty(),
            VERIFY_LIMIT,
            0L,
            Wei.ZERO,
            Bytes.EMPTY));
    frames.add(
        new Frame(Frame.MODE_SENDER, 0, Optional.of(recipient), 50_000L, 0L, Wei.ONE, Bytes.EMPTY));
    final FrameSignature placeholder =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 0, 65));
    final Transaction unsigned = build(sender, frames, List.of(placeholder));
    final SECPSignature signature =
        ALGO.sign(Bytes32.wrap(unsigned.getFrameSignatureHash().getBytes()), senderKeys);
    final Bytes raw =
        Bytes.concatenate(
            Bytes.of(signature.getRecId()),
            UInt256.valueOf(signature.getR()),
            UInt256.valueOf(signature.getS()));
    return build(
        sender,
        frames,
        List.of(
            new FrameSignature(FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, raw)));
  }

  /** A smart account whose code runs {@code body} and then approves execution and payment. */
  private Transaction smartAccountRunning(final String body) {
    final WorldUpdater setup = worldState.updater();
    setup
        .createAccount(smartAccount, 0, Wei.of(10).multiply(Wei.of(10).pow(18)))
        .setCode(Bytes.fromHexString(body + "600360006000aa"));
    setup.commit();
    return build(
        smartAccount,
        List.of(
            new Frame(
                Frame.MODE_VERIFY,
                Frame.APPROVE_SCOPE_MASK,
                Optional.empty(),
                VERIFY_LIMIT,
                0L,
                Wei.ZERO,
                Bytes.EMPTY)),
        List.of());
  }

  @Test
  void admitsASignedSelfRelay() {
    final Transaction transaction = selfRelay();
    final FrameTransactionAdmission.Result result = admission.admit(transaction, head);

    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
    final FrameTransactionAdmission.Admitted admitted = result.admitted().get();
    assertThat(admitted.payer()).isEqualTo(sender);
    assertThat(admitted.codedPaymaster()).isFalse();
    assertThat(admitted.maxCost()).isEqualTo(MAX_FEE.multiply(Wei.of(transaction.getGasLimit())));
    // Simulation must not leak into the head state.
    assertThat(worldState.get(sender).getNonce()).isZero();
  }

  @Test
  void admitsSmartAccountCodeThatReadsOnlyItsOwnStorage() {
    final FrameTransactionAdmission.Result result =
        admission.admit(smartAccountRunning("600054" + "50"), head); // SLOAD(0) POP
    assertThat(result.isAdmitted()).as(String.valueOf(result.message())).isTrue();
  }

  @Test
  void rejectsBannedOpcodesInTheValidationPrefix() {
    final FrameTransactionAdmission.Result result =
        admission.admit(smartAccountRunning("4250"), head); // TIMESTAMP POP
    assertThat(result.rejection())
        .isEqualTo(TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION);
    assertThat(result.message()).contains("TIMESTAMP");
  }

  @Test
  void rejectsReadsOfAnotherAccountsState() {
    final FrameTransactionAdmission.Result result =
        admission.admit(
            smartAccountRunning("73" + recipient.getBytes().toUnprefixedHexString() + "3b50"),
            head);
    // EXTCODESIZE of an account with no code.
    assertThat(result.rejection())
        .isEqualTo(TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION);
    assertThat(result.message()).contains("has no code");
  }

  @Test
  void rejectsAPrefixThatDoesNotApprove() {
    final FrameTransactionAdmission.Result result =
        admission.admit(smartAccountRunning("60006000fd"), head); // REVERT
    assertThat(result.isAdmitted()).isFalse();
    assertThat(result.rejection()).isEqualTo(TransactionInvalidReason.FRAME_VERIFICATION_FAILED);
  }

  @Test
  void rejectsUnrecognizedShapesWithoutSimulating() {
    final Transaction transaction =
        build(
            sender,
            List.of(
                new Frame(
                    Frame.MODE_SENDER,
                    0,
                    Optional.of(recipient),
                    50_000L,
                    0L,
                    Wei.ZERO,
                    Bytes.EMPTY)),
            List.of());
    final FrameTransactionAdmission.Result result = admission.admit(transaction, head);
    assertThat(result.rejection())
        .isEqualTo(TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION);
  }

  @Test
  void rejectsAnExpiredDeadline() {
    final FrameTransactionAdmission.Result result =
        admission.admit(selfRelay(ValidationPrefixTest.expiry(head.getTimestamp() - 1)), head);
    assertThat(result.rejection())
        .isEqualTo(TransactionInvalidReason.FRAME_MEMPOOL_POLICY_VIOLATION);
    assertThat(result.message()).contains("expiry");
  }
}
