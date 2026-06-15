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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes;

/** Transport abstraction for DiscV4 UDP send/receive. */
public interface V4Transport {

  CompletableFuture<InetSocketAddress> start();

  CompletableFuture<Void> stop();

  CompletableFuture<Void> send(InetSocketAddress recipient, Bytes data);

  void setInboundHandler(InboundV4Handler handler);

  @FunctionalInterface
  interface InboundV4Handler {
    void onPacket(InetSocketAddress sender, Bytes data);
  }
}
