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

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthMessage;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.messages.NewPooledTransactionHashesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NewPooledTransactionHashesMessageHandler implements EthMessages.MessageCallback {
  private static final Logger LOG =
      LoggerFactory.getLogger(NewPooledTransactionHashesMessageHandler.class);

  private final NewPooledTransactionHashesMessageProcessor transactionsMessageProcessor;
  private final EthScheduler scheduler;
  private final Duration txMsgKeepAlive;
  private final AtomicBoolean isEnabled = new AtomicBoolean(false);

  public NewPooledTransactionHashesMessageHandler(
      final EthScheduler scheduler,
      final NewPooledTransactionHashesMessageProcessor transactionsMessageProcessor,
      final int txMsgKeepAliveSeconds) {
    this.scheduler = scheduler;
    this.transactionsMessageProcessor = transactionsMessageProcessor;
    this.txMsgKeepAlive = Duration.ofSeconds(txMsgKeepAliveSeconds);
  }

  @Override
  public void exec(final EthMessage message) {
    if (isEnabled.get()) {
      final Capability capability = message.getPeer().getConnection().capability(EthProtocol.NAME);
      final MessageData rawMessage = message.getData();
      final Instant startedAt = now();
      scheduler.scheduleTxWorkerTask(
          () -> {
            if (message.getPeer().isDisconnected()) {
              return;
            }
            final NewPooledTransactionHashesMessage transactionsMessage;
            try {
              transactionsMessage =
                  NewPooledTransactionHashesMessage.readFrom(rawMessage, capability);
            } catch (final Exception e) {
              LOG.debug(
                  "Malformed pooled transaction hashes message received (BREACH_OF_PROTOCOL), disconnecting: {}",
                  message.getPeer(),
                  e);
              message
                  .getPeer()
                  .disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
              return;
            }
            transactionsMessageProcessor.processNewPooledTransactionHashesMessage(
                message.getPeer(), transactionsMessage, startedAt, txMsgKeepAlive);
          });
    }
  }

  public void setEnabled() {
    isEnabled.set(true);
  }

  public void setDisabled() {
    isEnabled.set(false);
  }

  public boolean isEnabled() {
    return isEnabled.get();
  }
}
