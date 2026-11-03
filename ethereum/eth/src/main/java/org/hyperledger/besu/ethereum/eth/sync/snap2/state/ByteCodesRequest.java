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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SnapMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Requests bytecodes by hash; returned codes keep the request order and may skip some. */
final class ByteCodesRequest implements SyncRequest<Map<Bytes32, Bytes>> {

  private final List<Bytes32> codeHashes;
  private final long bytes;

  ByteCodesRequest(final List<Bytes32> codeHashes, final long bytes) {
    this.codeHashes = List.copyOf(codeHashes);
    this.bytes = bytes;
  }

  @Override
  public Protocol protocol() {
    return Protocol.SNAP;
  }

  @Override
  public Priority priority() {
    return Priority.STATE;
  }

  @Override
  public String name() {
    return "bytecodes";
  }

  @Override
  public MessageData message(final EthPeer peer) {
    return SnapMessages.getByteCodes(codeHashes, bytes);
  }

  @Override
  public Map<Bytes32, Bytes> decode(final MessageData response, final EthPeer peer) {
    final Collection<Bytes> codes = ByteCodesMessage.readFrom(response).bytecodes(true).codes();
    if (codes.isEmpty()) {
      throw new EmptyResponseException("no bytecode");
    }
    final Map<Bytes32, Bytes> out = new LinkedHashMap<>();
    int next = 0;
    for (final Bytes code : codes) {
      final Bytes32 hash = Bytes32.wrap(Hash.hash(code).getBytes());
      while (next < codeHashes.size() && !codeHashes.get(next).equals(hash)) {
        next++;
      }
      if (next == codeHashes.size()) {
        throw new InvalidResponseException("unrequested or unordered bytecode", true);
      }
      out.put(hash, code);
      next++;
    }
    return out;
  }
}
