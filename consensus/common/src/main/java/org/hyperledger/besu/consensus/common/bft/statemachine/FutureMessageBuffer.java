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
package org.hyperledger.besu.consensus.common.bft.statemachine;

import org.hyperledger.besu.util.number.ByteUnits;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;

/**
 * Buffer which holds future BFT messages.
 *
 * <p>This buffer only allows messages to be added which have a chain height greater than current
 * height and up to chain futureMessagesMaxDistance from the current chain height.
 *
 * <p>When the total number of messages is greater futureMessagesLimit, or the total retained byte
 * size of buffered messages exceeds maxTotalMessageBytes, then messages are evicted.
 *
 * <p>If there is more than one height in the buffer then all messages for the highest chain height
 * are removed. Otherwise if there is only one height the oldest inserted message is removed.
 *
 * @param <T> the type of messages stored in this buffer
 */
public class FutureMessageBuffer<T> {

  /**
   * Default cap on the total byte size of messages retained across all buffered heights. A
   * message-count limit alone does not bound memory, since an individual BFT message (e.g. a QBFT
   * PROPOSAL carrying a full block) can be several megabytes.
   */
  @VisibleForTesting static final long DEFAULT_MAX_TOTAL_MESSAGE_BYTES = 64L * ByteUnits.MEGABYTE;

  private final NavigableMap<Long, List<T>> buffer = new TreeMap<>();
  private final long futureMessagesMaxDistance;
  private final long futureMessagesLimit;
  private final long maxTotalMessageBytes;
  private final ToLongFunction<T> messageSizeFn;
  private final FutureMessageHandler<T> futureMessageHandler;
  private long chainHeight;
  private long totalMessageBytes;

  /**
   * Future message handler, which is called when a future message is added to the buffer.
   *
   * @param <T> the type of message being handled
   */
  public interface FutureMessageHandler<T> {
    /**
     * Notify the handler of the future message being added to the buffer.
     *
     * @param msgChainHeight the msg chain height
     * @param message the message
     */
    void handleFutureMessage(long msgChainHeight, T message);
  }

  /**
   * Instantiates a new Future message buffer with an unbounded byte budget. Retained for callers
   * (mainly tests) that do not need byte-based eviction.
   *
   * @param futureMessagesMaxDistance the future messages max distance
   * @param futureMessagesLimit the future messages limit
   * @param chainHeight the chain height
   */
  public FutureMessageBuffer(
      final long futureMessagesMaxDistance,
      final long futureMessagesLimit,
      final long chainHeight) {
    this(
        futureMessagesMaxDistance,
        futureMessagesLimit,
        chainHeight,
        Long.MAX_VALUE,
        message -> 0L,
        (msgChainHeight, message) -> {});
  }

  /**
   * Instantiates a new Future message buffer with an unbounded byte budget. Retained for callers
   * (mainly tests) that do not need byte-based eviction.
   *
   * @param futureMessagesMaxDistance the future messages max distance
   * @param futureMessagesLimit the future messages limit
   * @param chainHeight the chain height
   * @param futureMessageHandler the future message handler
   */
  public FutureMessageBuffer(
      final long futureMessagesMaxDistance,
      final long futureMessagesLimit,
      final long chainHeight,
      final FutureMessageHandler<T> futureMessageHandler) {
    this(
        futureMessagesMaxDistance,
        futureMessagesLimit,
        chainHeight,
        Long.MAX_VALUE,
        message -> 0L,
        futureMessageHandler);
  }

  /**
   * Instantiates a new Future message buffer bounded by both message count and total retained byte
   * size, using the default byte budget.
   *
   * @param futureMessagesMaxDistance the future messages max distance
   * @param futureMessagesLimit the future messages limit
   * @param chainHeight the chain height
   * @param messageSizeFn function returning the byte size of a buffered message
   */
  public FutureMessageBuffer(
      final long futureMessagesMaxDistance,
      final long futureMessagesLimit,
      final long chainHeight,
      final ToLongFunction<T> messageSizeFn) {
    this(
        futureMessagesMaxDistance,
        futureMessagesLimit,
        chainHeight,
        DEFAULT_MAX_TOTAL_MESSAGE_BYTES,
        messageSizeFn,
        (msgChainHeight, message) -> {});
  }

  /**
   * Instantiates a new Future message buffer bounded by both message count and total retained byte
   * size (using the default byte budget), with a custom future message handler.
   *
   * @param futureMessagesMaxDistance the future messages max distance
   * @param futureMessagesLimit the future messages limit
   * @param chainHeight the chain height
   * @param messageSizeFn function returning the byte size of a buffered message
   * @param futureMessageHandler the future message handler
   */
  public FutureMessageBuffer(
      final long futureMessagesMaxDistance,
      final long futureMessagesLimit,
      final long chainHeight,
      final ToLongFunction<T> messageSizeFn,
      final FutureMessageHandler<T> futureMessageHandler) {
    this(
        futureMessagesMaxDistance,
        futureMessagesLimit,
        chainHeight,
        DEFAULT_MAX_TOTAL_MESSAGE_BYTES,
        messageSizeFn,
        futureMessageHandler);
  }

  /**
   * Instantiates a new Future message buffer bounded by both message count and total retained byte
   * size.
   *
   * @param futureMessagesMaxDistance the future messages max distance
   * @param futureMessagesLimit the future messages limit
   * @param chainHeight the chain height
   * @param maxTotalMessageBytes the maximum total byte size of buffered messages
   * @param messageSizeFn function returning the byte size of a buffered message
   * @param futureMessageHandler the future message handler
   */
  public FutureMessageBuffer(
      final long futureMessagesMaxDistance,
      final long futureMessagesLimit,
      final long chainHeight,
      final long maxTotalMessageBytes,
      final ToLongFunction<T> messageSizeFn,
      final FutureMessageHandler<T> futureMessageHandler) {
    this.futureMessagesMaxDistance = futureMessagesMaxDistance;
    this.futureMessagesLimit = futureMessagesLimit;
    this.chainHeight = chainHeight;
    this.maxTotalMessageBytes = maxTotalMessageBytes;
    this.messageSizeFn = messageSizeFn;
    this.futureMessageHandler = futureMessageHandler;
  }

  /**
   * Add message.
   *
   * @param msgChainHeight the msg chain height
   * @param rawMsg the message to add
   */
  public void addMessage(final long msgChainHeight, final T rawMsg) {
    futureMessageHandler.handleFutureMessage(msgChainHeight, rawMsg);

    if (futureMessagesLimit == 0 || !validMessageHeight(msgChainHeight, chainHeight)) {
      return;
    }

    addMessageToBuffer(msgChainHeight, rawMsg);

    while (totalMessagesSize() > 0
        && (totalMessagesSize() > futureMessagesLimit
            || totalMessageBytes > maxTotalMessageBytes)) {
      evictMessages();
    }
  }

  private void addMessageToBuffer(final long msgChainHeight, final T rawMsg) {
    buffer.putIfAbsent(msgChainHeight, new ArrayList<>());
    buffer.get(msgChainHeight).add(rawMsg);
    totalMessageBytes += messageSizeFn.applyAsLong(rawMsg);
  }

  private boolean validMessageHeight(final long msgChainHeight, final long currentHeight) {
    final boolean isFutureMsg = msgChainHeight > currentHeight;
    final boolean withinMaxChainHeight =
        msgChainHeight <= currentHeight + futureMessagesMaxDistance;
    return isFutureMsg && withinMaxChainHeight;
  }

  private void evictMessages() {
    if (buffer.size() > 1) {
      discardMessages(buffer.remove(buffer.lastKey()));
    } else if (buffer.size() == 1) {
      final List<T> messages = buffer.firstEntry().getValue();
      if (!messages.isEmpty()) {
        totalMessageBytes -= messageSizeFn.applyAsLong(messages.removeFirst());
      }
    }
  }

  /**
   * Retrieve messages for height.
   *
   * @param height the height
   * @return the list
   */
  public List<T> retrieveMessagesForHeight(final long height) {
    chainHeight = height;
    final List<T> messages = buffer.getOrDefault(height, Collections.emptyList());
    discardPreviousHeightMessages();
    return messages;
  }

  private void discardPreviousHeightMessages() {
    if (!buffer.isEmpty()) {
      for (long h = buffer.firstKey(); h <= chainHeight; h++) {
        discardMessages(buffer.remove(h));
      }
    }
  }

  private void discardMessages(final List<T> messages) {
    if (messages != null) {
      messages.forEach(message -> totalMessageBytes -= messageSizeFn.applyAsLong(message));
    }
  }

  /**
   * Total messages size.
   *
   * @return the long
   */
  @VisibleForTesting
  long totalMessagesSize() {
    return buffer.values().stream().map(List::size).reduce(0, Integer::sum).longValue();
  }

  /**
   * Total byte size of all messages currently retained in the buffer.
   *
   * @return the long
   */
  @VisibleForTesting
  long totalMessageBytes() {
    return totalMessageBytes;
  }
}
