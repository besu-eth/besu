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
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LAYERED;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LEGACY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.FrameSignature;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.frame.CompositePendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.testutil.TestClock;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End to end on a Bogota chain: the pool that {@link TransactionPoolFactory} builds admits a signed
 * EIP-8141 frame transaction, by simulating its validation prefix against the chain head, and
 * offers it to block selection next to the layered pool's transactions.
 */
class FrameTransactionPoolTest {

  private static final SignatureAlgorithm ALGO = SignatureAlgorithmFactory.getInstance();
  private static final BigInteger CHAIN_ID = BigInteger.ONE;
  private static final Address RECIPIENT =
      Address.fromHexString("0x00000000000000000000000000000000000000cc");
  // EIP-7906 assertion code that always fails: INVALID
  private static final Address FAILING_ASSERTION =
      Address.fromHexString("0x00000000000000000000000000000000000000ab");

  private static final String GENESIS =
      """
      {
        "config": {
          "chainId": 1,
          "homesteadBlock": 0,
          "eip150Block": 0,
          "eip155Block": 0,
          "eip158Block": 0,
          "byzantiumBlock": 0,
          "constantinopleBlock": 0,
          "petersburgBlock": 0,
          "istanbulBlock": 0,
          "berlinBlock": 0,
          "londonBlock": 0,
          "terminalTotalDifficulty": 0,
          "bogotaTime": 0,
          "depositContractAddress": "0x4242424242424242424242424242424242424242"
        },
        "nonce": "0x0",
        "timestamp": "0x1234",
        "extraData": "0x",
        "gasLimit": "0x1c9c380",
        "baseFeePerGas": "0x7",
        "difficulty": "0x0",
        "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "coinbase": "0x0000000000000000000000000000000000000000",
        "slotnumber": "0x42",
        "alloc": {
          "%s": { "balance": "0x8ac7230489e80000" },
          "0x00000000000000000000000000000000000000ab": { "balance": "0x0", "code": "0xfe" }
        }
      }
      """;

  private final KeyPair senderKeys = ALGO.generateKeyPair();
  private final Address sender = Address.extract(senderKeys.getPublicKey());
  private ExecutionContextTestFixture executionContext;

  @BeforeEach
  void setUp() {
    final GenesisConfig genesisConfig =
        GenesisConfig.fromConfig(GENESIS.formatted(sender.toHexString()));
    final ProtocolSchedule protocolSchedule =
        MainnetProtocolSchedule.fromConfig(
            genesisConfig.getConfigOptions(),
            Optional.of(false),
            Optional.of(EvmConfiguration.DEFAULT),
            MiningConfiguration.newDefault(),
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    executionContext =
        ExecutionContextTestFixture.builder(genesisConfig)
            .protocolSchedule(protocolSchedule)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
  }

  private TransactionPool createTransactionPool(
      final TransactionPoolConfiguration.Implementation implementation) {
    final EthScheduler ethScheduler = new DeterministicEthScheduler();
    final EthContext ethContext = mock(EthContext.class);
    when(ethContext.getEthPeers()).thenReturn(mock(EthPeers.class));
    when(ethContext.getEthMessages()).thenReturn(mock(EthMessages.class));
    when(ethContext.getScheduler()).thenReturn(ethScheduler);
    final SyncState syncState = mock(SyncState.class);
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);

    final TransactionPool transactionPool =
        TransactionPoolFactory.createTransactionPool(
            executionContext.getProtocolSchedule(),
            executionContext.getProtocolContext(),
            ethContext,
            TestClock.fixed(),
            new TransactionPoolMetrics(new NoOpMetricsSystem()),
            syncState,
            ImmutableTransactionPoolConfiguration.builder()
                .txPoolImplementation(implementation)
                .build(),
            mock(PeerTransactionTracker.class),
            mock(TransactionsMessageSender.class),
            mock(NewPooledTransactionHashesMessageSender.class),
            new BlobCache(),
            MiningConfiguration.newDefault(),
            EthProtocolConfiguration.DEFAULT,
            new TransactionEventNotifier(ethScheduler));
    assertThat(transactionPool.isEnabled()).isTrue();
    return transactionPool;
  }

  private static final Frame SELF_VERIFY =
      new Frame(
          Frame.MODE_VERIFY,
          Frame.APPROVE_SCOPE_MASK,
          Optional.empty(),
          50_000L,
          0L,
          Wei.ZERO,
          Bytes.EMPTY);
  private static final Frame TRANSFER_ONE_WEI =
      new Frame(Frame.MODE_SENDER, 0, Optional.of(RECIPIENT), 50_000L, 0L, Wei.ONE, Bytes.EMPTY);
  private static final Frame FAILING_POST_TX =
      new Frame(
          Frame.MODE_POST_TX,
          0,
          Optional.of(FAILING_ASSERTION),
          50_000L,
          0L,
          Wei.ZERO,
          Bytes.EMPTY);

  private Transaction buildFrameTransaction(
      final long nonce,
      final long priorityFee,
      final List<Frame> frames,
      final List<FrameSignature> signatures) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonce(nonce)
        .sender(sender)
        .frames(frames)
        .frameSignatures(signatures)
        .maxPriorityFeePerGas(Wei.of(priorityFee))
        .maxFeePerGas(Wei.of(priorityFee + 1_000))
        .maxFeePerBlobGas(Wei.ZERO)
        .build();
  }

  /** A self-relayed transfer: the sender verifies itself and pays, with one secp256k1 entry. */
  private Transaction selfRelay(final long nonce, final long priorityFee) {
    return selfRelay(nonce, priorityFee, List.of(SELF_VERIFY, TRANSFER_ONE_WEI));
  }

  /** The given frames from the sender, canonically signed with one secp256k1 entry. */
  private Transaction selfRelay(
      final long nonce, final long priorityFee, final List<Frame> frames) {
    final FrameSignature placeholder =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 0, 65));
    final Transaction unsigned =
        buildFrameTransaction(nonce, priorityFee, frames, List.of(placeholder));
    final SECPSignature signature =
        ALGO.sign(Bytes32.wrap(unsigned.getFrameSignatureHash().getBytes()), senderKeys);
    final Bytes raw =
        Bytes.concatenate(
            Bytes.of(signature.getRecId()),
            UInt256.valueOf(signature.getR()),
            UInt256.valueOf(signature.getS()));
    return buildFrameTransaction(
        nonce,
        priorityFee,
        frames,
        List.of(
            new FrameSignature(FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, raw)));
  }

  private Transaction transfer(final long nonce, final long priorityFee) {
    return new TransactionTestFixture()
        .type(TransactionType.EIP1559)
        .chainId(Optional.of(CHAIN_ID))
        .nonce(nonce)
        .gasLimit(100_000L)
        .to(Optional.of(RECIPIENT))
        .maxPriorityFeePerGas(Optional.of(Wei.of(priorityFee)))
        .maxFeePerGas(Optional.of(Wei.of(priorityFee + 1_000)))
        .createTransaction(senderKeys);
  }

  @Test
  void admitsAFrameTransactionAndOffersItToBlockSelection() {
    final TransactionPool transactionPool = createTransactionPool(LAYERED);
    assertThat(transactionPool.pendingTransactionsImplementation())
        .isEqualTo(CompositePendingTransactions.class);

    final Transaction frameTransaction = selfRelay(0, 2_000);
    final ValidationResult<TransactionInvalidReason> result =
        transactionPool.addTransactionViaApi(frameTransaction);

    assertThat(result.isValid()).as(result.toString()).isTrue();
    assertThat(transactionPool.count()).isEqualTo(1);
    assertThat(transactionPool.getTransactionByHash(frameTransaction.getHash()))
        .contains(frameTransaction);
    assertThat(transactionPool.getPendingTransactionsFor(sender).pendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsExactly(frameTransaction);
    assertThat(transactionPool.logStats()).endsWith(" | Frame 1/1024");
    // the simulation did not touch the head state
    assertThat(executionContext.getStateArchive().getWorldState().get(sender).getNonce()).isZero();

    final List<Transaction> offered = new ArrayList<>();
    transactionPool.selectTransactions(
        candidates -> {
          candidates.forEach(candidate -> offered.add(candidate.getTransaction()));
          return candidates.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(), candidate -> TransactionSelectionResult.SELECTED));
        });
    assertThat(offered).containsExactly(frameTransaction);
    assertThat(transactionPool.count()).isEqualTo(1);
  }

  @Test
  void offersFrameAndLayeredTransactionsInOneSelectionPass() {
    final TransactionPool transactionPool = createTransactionPool(LAYERED);
    final Transaction frameTransaction = selfRelay(0, 2_000);
    final Transaction transfer = transfer(0, 3_000);

    assertThat(transactionPool.addTransactionViaApi(frameTransaction).isValid()).isTrue();
    assertThat(transactionPool.addTransactionViaApi(transfer).isValid()).isTrue();
    assertThat(transactionPool.count()).isEqualTo(2);

    final List<List<Transaction>> passes = new ArrayList<>();
    transactionPool.selectTransactions(
        candidates -> {
          passes.add(candidates.stream().map(PendingTransaction::getTransaction).toList());
          return Map.of();
        });
    assertThat(passes).containsExactly(List.of(transfer, frameTransaction));
  }

  @Test
  void rejectsAFrameTransactionThatFailsAdmission() {
    final TransactionPool transactionPool = createTransactionPool(LAYERED);

    // the sender's nonce is 0: admission simulates the prefix with the exact nonce
    final ValidationResult<TransactionInvalidReason> result =
        transactionPool.addTransactionViaApi(selfRelay(1, 2_000));

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason()).isEqualTo(TransactionInvalidReason.NONCE_TOO_HIGH);
    assertThat(transactionPool.count()).isZero();
  }

  @Test
  void keepsOnePendingFrameTransactionPerSender() {
    final TransactionPool transactionPool = createTransactionPool(LAYERED);
    assertThat(transactionPool.addTransactionViaApi(selfRelay(0, 2_000)).isValid()).isTrue();

    // the same nonce identity replaces it only with both fee caps bumped
    final ValidationResult<TransactionInvalidReason> underpriced =
        transactionPool.addTransactionViaApi(selfRelay(0, 2_001));
    assertThat(underpriced.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.TRANSACTION_REPLACEMENT_UNDERPRICED);

    final Transaction replacement = selfRelay(0, 4_000);
    assertThat(transactionPool.addTransactionViaApi(replacement).isValid()).isTrue();
    assertThat(transactionPool.getPendingTransactionsFor(sender).pendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsExactly(replacement);
  }

  @Test
  void aSorterBasedPoolStillRejectsFrameTransactions() {
    final TransactionPool transactionPool = createTransactionPool(LEGACY);
    assertThat(transactionPool.pendingTransactionsImplementation())
        .isEqualTo(BaseFeePendingTransactionsSorter.class);

    final ValidationResult<TransactionInvalidReason> result =
        transactionPool.addTransactionViaApi(selfRelay(0, 2_000));

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason())
        .isEqualTo(TransactionInvalidReason.INVALID_TRANSACTION_FORMAT);
    assertThat(result.getErrorMessage())
        .isEqualTo("Frame transactions are not supported by the configured transaction pool");
    assertThat(transactionPool.count()).isZero();
  }

  @Test
  void admitsAFrameTransactionWhoseAssertionWouldFail() {
    final TransactionPool transactionPool = createTransactionPool(LAYERED);
    // geth TestPostTxAdmission: the pool simulates only the validation prefix, so the INVALID
    // assertion never runs. In a block it would fail, revert the transfer and leave the
    // transaction valid and paid.
    final Transaction frameTransaction =
        selfRelay(0, 2_000, List.of(SELF_VERIFY, TRANSFER_ONE_WEI, FAILING_POST_TX));

    final ValidationResult<TransactionInvalidReason> result =
        transactionPool.addTransactionViaApi(frameTransaction);

    assertThat(result.isValid()).as(result.toString()).isTrue();
    assertThat(transactionPool.getPendingTransactionsFor(sender).pendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsExactly(frameTransaction);
    assertThat(transactionPool.logStats()).endsWith(" | Frame 1/1024");
    assertThat(executionContext.getStateArchive().getWorldState().get(sender).getNonce()).isZero();
  }

  @Test
  void rejectsAFrameAfterAPostTxFrameBeforeAdmission() {
    final TransactionPool transactionPool = createTransactionPool(LAYERED);

    // The fork's validator applies the EIP-7906 static rules before the pool classifies the
    // prefix, and JSON-RPC reports INVALID_FRAMES as FRAME_VALIDATION_FAILED.
    final ValidationResult<TransactionInvalidReason> result =
        transactionPool.addTransactionViaApi(
            selfRelay(0, 2_000, List.of(SELF_VERIFY, FAILING_POST_TX, TRANSFER_ONE_WEI)));

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason()).isEqualTo(TransactionInvalidReason.INVALID_FRAMES);
    assertThat(result.getErrorMessage()).isEqualTo("frame 2 follows a POST_TX frame");
    assertThat(transactionPool.count()).isZero();
  }
}
