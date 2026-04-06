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
package org.hyperledger.besu.ethereum.p2p.peers;

import org.hyperledger.besu.ethereum.forkid.ForkId;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/** The default, basic representation of an Ethereum {@link Peer}. */
public class DefaultPeer extends DefaultPeerId implements Peer {

  private final EnodeURLImpl enodeURL;
  private ForkId forkId;

  protected DefaultPeer(final EnodeURLImpl enodeURL) {
    super(enodeURL.getNodeId());
    this.enodeURL = enodeURL;
  }

  public static DefaultPeer fromEnodeURL(final EnodeURLImpl enodeURL) {
    return new DefaultPeer(enodeURL);
  }

  /**
   * Creates a {@link DefaultPeer} instance from a String representation of an enode URL.
   *
   * @param uri A String representation of the enode URI.
   * @return The Peer instance.
   * @see <a href="https://github.com/ethereum/wiki/wiki/enode-url-format">enode URL format</a>
   */
  public static DefaultPeer fromURI(final String uri) {
    return new DefaultPeer(EnodeURLImpl.fromString(uri));
  }

  /**
   * Creates a {@link DefaultPeer} instance from an URI object that follows the enode URL format.
   *
   * @param uri The enode URI.
   * @return The Peer instance.
   * @see <a href="https://github.com/ethereum/wiki/wiki/enode-url-format">enode URL format</a>
   */
  public static DefaultPeer fromURI(final URI uri) {
    return new DefaultPeer(EnodeURLImpl.fromURI(uri));
  }

  @Override
  public EnodeURLImpl getEnodeURL() {
    return enodeURL;
  }

  @Override
  public Optional<ForkId> getForkId() {
    return Optional.ofNullable(forkId);
  }

  @Override
  public void setForkId(final ForkId forkId) {
    this.forkId = forkId;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof DefaultPeer)) {
      return false;
    }
    final DefaultPeer other = (DefaultPeer) obj;
    return id.equals(other.id) && enodeURL.equals(other.enodeURL);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, enodeURL);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DefaultPeer{");
    sb.append("id=").append(id);
    sb.append(", enode=").append(enodeURL);
    sb.append('}');
    return sb.toString();
  }
}
