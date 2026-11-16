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

import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestScheduler;

import java.util.Optional;

public class EthContext {

  private final EthPeers ethPeers;
  private final EthMessages ethMessages;
  private final Optional<EthMessages> snapMessages;
  private final EthScheduler scheduler;
  private final PeerTaskExecutor peerTaskExecutor;
  private final RequestScheduler requestScheduler;

  public EthContext(
      final EthPeers ethPeers,
      final EthMessages ethMessages,
      final EthMessages snapMessages,
      final EthScheduler scheduler,
      final PeerTaskExecutor peerTaskExecutor,
      final RequestScheduler requestScheduler) {
    this.ethPeers = ethPeers;
    this.ethMessages = ethMessages;
    this.snapMessages = Optional.of(snapMessages);
    this.scheduler = scheduler;
    this.peerTaskExecutor = peerTaskExecutor;
    this.requestScheduler = requestScheduler;
  }

  public EthContext(
      final EthPeers ethPeers,
      final EthMessages ethMessages,
      final EthScheduler scheduler,
      final PeerTaskExecutor peerTaskExecutor) {
    this.ethPeers = ethPeers;
    this.ethMessages = ethMessages;
    this.snapMessages = Optional.empty();
    this.scheduler = scheduler;
    this.peerTaskExecutor = peerTaskExecutor;
    this.requestScheduler = null;
  }

  public EthPeers getEthPeers() {
    return ethPeers;
  }

  public EthMessages getEthMessages() {
    return ethMessages;
  }

  public Optional<EthMessages> getSnapMessages() {
    return snapMessages;
  }

  public EthScheduler getScheduler() {
    return scheduler;
  }

  public PeerTaskExecutor getPeerTaskExecutor() {
    return peerTaskExecutor;
  }

  /** Returns the request scheduler that we use to send requests through. */
  public RequestScheduler getRequestScheduler() {
    return requestScheduler;
  }
}
