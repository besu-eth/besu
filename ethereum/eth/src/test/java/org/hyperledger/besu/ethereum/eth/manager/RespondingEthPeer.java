/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.manager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.eth.core.Utils.serializeReceiptsList;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.core.Utils;
import org.hyperledger.besu.ethereum.eth.manager.snap.SnapProtocolManager;
import org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.NodeDataMessage;
import org.hyperledger.besu.ethereum.eth.messages.PooledTransactionsMessage;
import org.hyperledger.besu.ethereum.eth.messages.ReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.StatusMessage;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.DefaultMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;

public class RespondingEthPeer {
  private static final BlockDataGenerator gen = new BlockDataGenerator();
  private final EthPeer ethPeer;
  private final BlockingQueue<OutgoingMessage> outgoingMessages;
  private final EthProtocolManager ethProtocolManager;
  private final Optional<SnapProtocolManager> snapProtocolManager;
  private final MockPeerConnection peerConnection;

  private RespondingEthPeer(
      final EthProtocolManager ethProtocolManager,
      final Optional<SnapProtocolManager> snapProtocolManager,
      final MockPeerConnection peerConnection,
      final EthPeer ethPeer,
      final BlockingQueue<OutgoingMessage> outgoingMessages) {
    this.ethProtocolManager = ethProtocolManager;
    this.snapProtocolManager = snapProtocolManager;
    this.peerConnection = peerConnection;
    this.ethPeer = ethPeer;
    this.outgoingMessages = outgoingMessages;
  }

  public static void respondOnce(final Responder responder, final List<RespondingEthPeer> peers) {
    for (final RespondingEthPeer peer : peers) {
      if (peer.respond(responder)) {
        break;
      }
    }
  }

  public static void respondOnce(final Responder responder, final RespondingEthPeer... peers) {
    respondOnce(responder, Arrays.asList(peers));
  }

  public boolean disconnect(final DisconnectReason reason) {
    if (ethPeer.isDisconnected()) {
      return false;
    }

    ethPeer.disconnect(reason);
    ethProtocolManager.handleDisconnect(getPeerConnection(), reason, true);
    return true;
  }

  public MockPeerConnection getPeerConnection() {
    return peerConnection;
  }

  public static Builder builder() {
    return new Builder();
  }

  private static RespondingEthPeer create(
      final EthProtocolManager ethProtocolManager,
      final Optional<SnapProtocolManager> snapProtocolManager,
      final Capability capability,
      final Hash chainHeadHash,
      final Difficulty totalDifficulty,
      final OptionalLong estimatedHeight,
      final List<PeerValidator> peerValidators,
      final boolean isServingSnap,
      final boolean addToEthPeers) {
    final EthPeers ethPeers = ethProtocolManager.ethContext().getEthPeers();

    final Set<Capability> caps = new HashSet<>(Collections.singletonList(capability));
    final BlockingQueue<OutgoingMessage> outgoingMessages = new ArrayBlockingQueue<>(1000);
    final MockPeerConnection peerConnection =
        new MockPeerConnection(
            caps, (cap, msg, conn) -> outgoingMessages.add(new OutgoingMessage(cap, msg)));
    ethPeers.registerNewConnection(peerConnection, peerValidators);
    final int before = ethPeers.peerCount();
    final EthPeer peer = ethPeers.peer(peerConnection);

    StatusMessage.Builder statusMessageBuilder =
        StatusMessage.builder()
            .protocolVersion(capability.getVersion())
            .networkId(BigInteger.ONE)
            .genesisHash(gen.hash())
            .bestHash(chainHeadHash)
            .forkId(new ForkId(Hash.ZERO.getBytes(), 0));
    if (capability.getVersion() < EthProtocolVersion.V69) {
      statusMessageBuilder.totalDifficulty(totalDifficulty);
    } else if (EthProtocol.isEth69Compatible(capability)) {
      statusMessageBuilder.blockRange(new StatusMessage.BlockRange(0, estimatedHeight.orElse(0)));
    }
    StatusMessage statusMessage = statusMessageBuilder.build();

    peer.registerStatusReceived(statusMessage, peerConnection);
    estimatedHeight.ifPresent(height -> peer.chainState().update(chainHeadHash, height));
    if (addToEthPeers) {
      peer.registerStatusSent(peerConnection);
      // Don't call addPeerToEthPeers directly — let ethPeerStatusExchanged handle it
      // so that connect callbacks fire properly (needed for waitForPeer subscriptions).
      while (ethPeers.peerCount()
          <= before) { // this is needed to make sure that the peer is added to the active
        // connections
        try {
          Thread.sleep(100L);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    peer.setIsServingSnap(isServingSnap);

    return new RespondingEthPeer(
        ethProtocolManager, snapProtocolManager, peerConnection, peer, outgoingMessages);
  }

  public EthPeer getEthPeer() {
    return ethPeer;
  }

  public void respondWhile(final Responder responder, final RespondWhileCondition condition) {
    int counter = 0;
    while (condition.shouldRespond()) {
      respond(responder);
      counter++;
      if (counter > 10_000) {
        // Limit applied to avoid tests hanging forever which is hard to track down.
        throw new IllegalStateException(
            "Responded 10,000 times and stop condition still not reached.");
      }
    }
  }

  public void respondWhileOtherThreadsWork(
      final Responder responder, final RespondWhileCondition condition) {
    int counter = 0;
    while (condition.shouldRespond()) {
      try {
        final OutgoingMessage message = outgoingMessages.poll(1, TimeUnit.SECONDS);
        if (message != null) {
          respondToMessage(responder, message);
          counter++;
          if (counter > 10_000) {
            // Limit applied to avoid tests hanging forever which is hard to track down.
            throw new IllegalStateException(
                "Responded 10,000 times and stop condition still not reached.");
          }
        }
      } catch (final InterruptedException e) {
        // Ignore and recheck condition.
      }
    }
  }

  public void respondTimes(final Responder responder, final int maxCycles) {
    // Respond repeatedly, as each round may produce new outgoing messages
    int count = 0;
    while (!outgoingMessages.isEmpty()) {
      count++;
      respond(responder);
      if (count >= maxCycles) {
        break;
      }
    }
  }

  /**
   * @return True if any requests were processed
   */
  public boolean respond(final Responder responder) {
    // Respond to queued messages
    final List<OutgoingMessage> currentMessages = new ArrayList<>();
    outgoingMessages.drainTo(currentMessages);
    for (final OutgoingMessage msg : currentMessages) {
      respondToMessage(responder, msg);
    }
    return currentMessages.size() > 0;
  }

  private void respondToMessage(final Responder responder, final OutgoingMessage msg) {
    boolean supportsRequestId = EthProtocol.requestIdCompatible(msg.messageData.getCode());
    final Optional<MessageData> maybeResponse =
        responder.respond(
            msg.capability,
            ethPeer,
            supportsRequestId ? msg.messageData.unwrapMessageData().getValue() : msg.messageData);
    maybeResponse.ifPresent(
        (response) -> {
          if (ethProtocolManager.getSupportedCapabilities().contains(msg.capability)) {
            ethProtocolManager.processMessage(
                msg.capability,
                new DefaultMessage(
                    peerConnection,
                    supportsRequestId
                        ? response.wrapMessageData(msg.messageData.unwrapMessageData().getKey())
                        : response));
          } else
            snapProtocolManager.ifPresent(
                protocolManager ->
                    protocolManager.processMessage(
                        msg.capability, new DefaultMessage(peerConnection, response)));
        });
  }

  public Optional<MessageData> peekNextOutgoingRequest() {
    if (outgoingMessages.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(outgoingMessages.peek().messageData);
  }

  public Stream<MessageData> streamPendingOutgoingRequests() {
    return outgoingMessages.stream().map(OutgoingMessage::messageData);
  }

  public boolean hasOutstandingRequests() {
    return !outgoingMessages.isEmpty();
  }

  public static Responder targetedResponder(
      final RequestFilter requestFilter, final ResponseGenerator responseGenerator) {
    return (cap, peer, msg) -> {
      if (requestFilter.filter(cap, peer, msg)) {
        return Optional.of(responseGenerator.respond(cap, peer, msg));
      } else {
        return Optional.empty();
      }
    };
  }

  private static TransactionPool createTransactionPool() {
    return mock(TransactionPool.class);
  }

  public static Responder blockchainResponder(
      final Blockchain blockchain, final WorldStateArchive worldStateArchive) {
    return blockchainResponder(blockchain, worldStateArchive, createTransactionPool());
  }

  public static Responder blockchainResponder(final Blockchain blockchain) {
    return blockchainResponder(
        blockchain, createInMemoryWorldStateArchive(), createTransactionPool());
  }

  public static Responder blockchainResponder(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final TransactionPool transactionPool) {
    final int maxMsgSize = EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE;
    return (cap, peer, msg) -> {
      MessageData response = null;
      switch (msg.getCode()) {
        case EthProtocolMessages.GET_BLOCK_HEADERS:
          response = EthServer.constructGetHeadersResponse(blockchain, msg, 200, maxMsgSize);
          break;
        case EthProtocolMessages.GET_BLOCK_BODIES:
          response = EthServer.constructGetBodiesResponse(blockchain, msg, 200, maxMsgSize);
          break;
        case EthProtocolMessages.GET_RECEIPTS:
          response = EthServer.constructGetReceiptsResponse(blockchain, msg, 200, maxMsgSize, cap);
          break;
        case EthProtocolMessages.GET_NODE_DATA:
          response =
              EthServer.constructGetNodeDataResponse(worldStateArchive, msg, 200, maxMsgSize);
          break;
        case EthProtocolMessages.GET_POOLED_TRANSACTIONS:
          response =
              EthServer.constructGetPooledTransactionsResponse(
                  transactionPool, peer, msg, 200, maxMsgSize);
          break;
        case EthProtocolMessages.GET_BLOCK_ACCESS_LISTS:
          response =
              EthServer.constructGetBlockAccessListsResponse(blockchain, msg, 200, maxMsgSize);
      }
      return Optional.ofNullable(response);
    };
  }

  public static Responder wrapResponderWithCollector(
      final Responder responder, final List<MessageData> messageCollector) {
    return (cap, peer, msg) -> {
      messageCollector.add(msg);
      return responder.respond(cap, peer, msg);
    };
  }

  /**
   * Create a responder that only responds with a fixed portion of the available data.
   *
   * @param portion The portion of the available data to return, from 0 to 1
   */
  public static Responder partialResponder(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final TransactionPool transactionPool,
      final ProtocolSchedule protocolSchedule,
      final float portion) {
    checkArgument(portion >= 0.0 && portion <= 1.0, "Portion is not in the range [0.0..1.0]");

    final Responder fullResponder =
        blockchainResponder(blockchain, worldStateArchive, transactionPool);
    return (cap, peer, msg) -> {
      final Optional<MessageData> maybeResponse = fullResponder.respond(cap, peer, msg);
      if (!maybeResponse.isPresent()) {
        return maybeResponse;
      }
      // Rewrite response with a subset of data
      final MessageData originalResponse = maybeResponse.get();
      MessageData partialResponse = originalResponse;
      switch (msg.getCode()) {
        case EthProtocolMessages.GET_BLOCK_HEADERS:
          final BlockHeadersMessage headersMessage = BlockHeadersMessage.readFrom(originalResponse);
          final List<BlockHeader> originalHeaders =
              Lists.newArrayList(headersMessage.getHeaders(protocolSchedule));
          final List<BlockHeader> partialHeaders =
              originalHeaders.subList(0, (int) (originalHeaders.size() * portion));
          partialResponse = BlockHeadersMessage.create(partialHeaders);
          break;
        case EthProtocolMessages.GET_BLOCK_BODIES:
          final BlockBodiesMessage bodiesMessage = BlockBodiesMessage.readFrom(originalResponse);
          final List<BlockBody> originalBodies =
              Lists.newArrayList(bodiesMessage.bodies(protocolSchedule));
          final List<BlockBody> partialBodies =
              originalBodies.subList(0, (int) (originalBodies.size() * portion));
          partialResponse = BlockBodiesMessage.create(partialBodies);
          break;
        case EthProtocolMessages.GET_RECEIPTS:
          final ReceiptsMessage receiptsMessage = ReceiptsMessage.readFrom(originalResponse);
          final List<List<TransactionReceipt>> originalReceipts =
              receiptsMessage.syncReceipts().stream().map(Utils::syncReceiptsToReceipts).toList();
          final List<List<TransactionReceipt>> partialReceipts =
              originalReceipts.subList(0, (int) (originalReceipts.size() * portion));
          partialResponse =
              ReceiptsMessage.createUnsafe(
                  serializeReceiptsList(
                      partialReceipts,
                      TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));
          break;
        case EthProtocolMessages.GET_NODE_DATA:
          final NodeDataMessage nodeDataMessage = NodeDataMessage.readFrom(originalResponse);
          final List<Bytes> originalNodeData = Lists.newArrayList(nodeDataMessage.nodeData());
          final List<Bytes> partialNodeData =
              originalNodeData.subList(0, (int) (originalNodeData.size() * portion));
          partialResponse = NodeDataMessage.create(partialNodeData);
          break;
        case EthProtocolMessages.GET_POOLED_TRANSACTIONS:
          final PooledTransactionsMessage pooledTransactionsMessage =
              PooledTransactionsMessage.readFrom(originalResponse);
          final List<Transaction> originalPooledTx =
              Lists.newArrayList(pooledTransactionsMessage.transactions());
          final List<Transaction> partialPooledTx =
              originalPooledTx.subList(0, (int) (originalPooledTx.size() * portion));
          partialResponse = PooledTransactionsMessage.create(partialPooledTx);
          break;
      }
      return Optional.of(partialResponse);
    };
  }

  public static Responder emptyResponder() {
    return (cap, peer, msg) -> {
      MessageData response = null;
      switch (msg.getCode()) {
        case EthProtocolMessages.GET_BLOCK_HEADERS:
          response = BlockHeadersMessage.create(Collections.emptyList());
          break;
        case EthProtocolMessages.GET_BLOCK_BODIES:
          response = BlockBodiesMessage.create(Collections.emptyList());
          break;
        case EthProtocolMessages.GET_RECEIPTS:
          response =
              ReceiptsMessage.createUnsafe(
                  serializeReceiptsList(
                      Collections.emptyList(),
                      TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));
          break;
        case EthProtocolMessages.GET_NODE_DATA:
          response = NodeDataMessage.create(Collections.emptyList());
          break;
        case EthProtocolMessages.GET_POOLED_TRANSACTIONS:
          response = PooledTransactionsMessage.create(Collections.emptyList());
          break;
        case EthProtocolMessages.GET_BLOCK_ACCESS_LISTS:
          response = BlockAccessListsMessage.create(Collections.emptyList());
          break;
      }
      return Optional.ofNullable(response);
    };
  }

  public static class Builder {
    private EthProtocolManager ethProtocolManager;
    private Optional<SnapProtocolManager> snapProtocolManager = Optional.empty();
    private Hash chainHeadHash = gen.hash();
    private Difficulty totalDifficulty = Difficulty.of(1000L);
    private OptionalLong estimatedHeight = OptionalLong.of(1000L);
    private final List<PeerValidator> peerValidators = new ArrayList<>();
    private boolean isServingSnap = false;
    private boolean addToEthPeers = true;
    private Capability capability = EthProtocol.LATEST;

    public RespondingEthPeer build() {
      checkNotNull(ethProtocolManager, "Must configure EthProtocolManager");
      checkNotNull(capability, "Must configure Capability");

      return RespondingEthPeer.create(
          ethProtocolManager,
          snapProtocolManager,
          capability,
          chainHeadHash,
          totalDifficulty,
          estimatedHeight,
          peerValidators,
          isServingSnap,
          addToEthPeers);
    }

    public Builder ethProtocolManager(final EthProtocolManager ethProtocolManager) {
      checkNotNull(ethProtocolManager);
      this.ethProtocolManager = ethProtocolManager;
      return this;
    }

    public Builder snapProtocolManager(final SnapProtocolManager snapProtocolManager) {
      checkNotNull(snapProtocolManager);
      this.snapProtocolManager = Optional.of(snapProtocolManager);
      return this;
    }

    public Builder chainHeadHash(final Hash chainHeadHash) {
      checkNotNull(chainHeadHash);
      this.chainHeadHash = chainHeadHash;
      return this;
    }

    public Builder totalDifficulty(final Difficulty totalDifficulty) {
      checkNotNull(totalDifficulty);
      this.totalDifficulty = totalDifficulty;
      return this;
    }

    public Builder estimatedHeight(final OptionalLong estimatedHeight) {
      checkNotNull(estimatedHeight);
      this.estimatedHeight = estimatedHeight;
      return this;
    }

    public Builder estimatedHeight(final long estimatedHeight) {
      this.estimatedHeight = OptionalLong.of(estimatedHeight);
      return this;
    }

    public Builder isServingSnap(final boolean isServingSnap) {
      this.isServingSnap = isServingSnap;
      return this;
    }

    public Builder peerValidators(final List<PeerValidator> peerValidators) {
      checkNotNull(peerValidators);
      this.peerValidators.addAll(peerValidators);
      return this;
    }

    public Builder peerValidators(final PeerValidator... peerValidators) {
      peerValidators(Arrays.asList(peerValidators));
      return this;
    }

    public Builder addToEthPeers(final boolean addToEthPeers) {
      this.addToEthPeers = addToEthPeers;
      return this;
    }

    public Builder capability(final Capability capability) {
      this.capability = capability;
      return this;
    }
  }

  record OutgoingMessage(Capability capability, MessageData messageData) {}

  @FunctionalInterface
  public interface Responder {
    Optional<MessageData> respond(Capability cap, EthPeer peer, MessageData msg);
  }

  public interface RespondWhileCondition {
    boolean shouldRespond();
  }

  public interface RequestFilter {
    boolean filter(Capability cap, EthPeer peer, MessageData msg);
  }

  public interface ResponseGenerator {
    MessageData respond(Capability cap, EthPeer peer, MessageData msg);
  }
}
