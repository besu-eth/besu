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
package org.hyperledger.besu.ethereum.eth.transactions;

import static java.time.Instant.now;

import org.hyperledger.besu.ethereum.eth.manager.EthMessage;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.messages.TransactionsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionsMessageHandler implements EthMessages.MessageCallback {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionsMessageHandler.class);

  private final TransactionsMessageProcessor transactionsMessageProcessor;
  private final EthScheduler scheduler;
  private final Duration txMsgKeepAlive;
  private final int maxMessageSize;
  private final AtomicBoolean isEnabled = new AtomicBoolean(false);

  public TransactionsMessageHandler(
      final EthScheduler scheduler,
      final TransactionsMessageProcessor transactionsMessageProcessor,
      final int txMsgKeepAliveSeconds,
      final int maxMessageSize) {
    this.scheduler = scheduler;
    this.transactionsMessageProcessor = transactionsMessageProcessor;
    this.txMsgKeepAlive = Duration.ofSeconds(txMsgKeepAliveSeconds);
    this.maxMessageSize = maxMessageSize;
  }

  @Override
  public void exec(final EthMessage message) {
    if (isEnabled.get()) {
      final MessageData rawMessage = message.getData();
      if (rawMessage.getSize() > maxMessageSize) {
        LOG.debug(
            "Oversized transactions message received ({} bytes), disconnecting: {}",
            rawMessage.getSize(),
            message.getPeer());
        message
            .getPeer()
            .disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
        return;
      }
      final Instant startedAt = now();
      scheduler.scheduleTxWorkerTask(
          () -> {
            if (message.getPeer().isDisconnected()) {
              return;
            }
            final TransactionsMessage transactionsMessage;
            try {
              transactionsMessage = TransactionsMessage.readFrom(rawMessage);
            } catch (final Exception e) {
              LOG.debug(
                  "Malformed transactions message received (BREACH_OF_PROTOCOL), disconnecting: {}",
                  message.getPeer(),
                  e);
              message
                  .getPeer()
                  .disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
              return;
            }
            transactionsMessageProcessor.processTransactionsMessage(
                message.getPeer(), transactionsMessage, startedAt, txMsgKeepAlive);
          });
    }
  }

  public void setDisabled() {
    isEnabled.set(false);
  }

  public void setEnabled() {
    isEnabled.set(true);
  }

  public boolean isEnabled() {
    return isEnabled.get();
  }
}
