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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryPacketDecodingException;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.util.IllegalPortException;

import java.net.InetAddress;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class EndpointTest {

  // A value written immediately after the endpoint in every test's enclosing list (mirroring a
  // PING packet's "expiration" field following "to"), used to prove a malformed endpoint doesn't
  // desync the cursor for whatever comes after it.
  private static final long MARKER_VALUE = 918273645L;

  @Test
  public void decodeStandalone_twoFieldEndpoint_decodesCorrectly() throws Exception {
    final InetAddress host = InetAddress.getByName("10.0.0.1");
    final BytesValueRLPInput in = encode(out -> writeEndpoint(out, host, 30303, Optional.empty()));

    final Endpoint endpoint = Endpoint.decodeStandalone(in);

    assertThat(endpoint).isEqualTo(new Endpoint("10.0.0.1", 30303, Optional.empty()));
    assertThat(in.readLongScalar()).isEqualTo(MARKER_VALUE);
  }

  @Test
  public void decodeStandalone_threeFieldEndpoint_decodesCorrectly() throws Exception {
    final InetAddress host = InetAddress.getByName("10.0.0.1");
    final BytesValueRLPInput in =
        encode(out -> writeEndpoint(out, host, 30303, Optional.of(30304)));

    final Endpoint endpoint = Endpoint.decodeStandalone(in);

    assertThat(endpoint).isEqualTo(new Endpoint("10.0.0.1", 30303, Optional.of(30304)));
    assertThat(in.readLongScalar()).isEqualTo(MARKER_VALUE);
  }

  @Test
  public void decodeStandalone_malformedFieldCount_throwsAndKeepsCursorInSync() {
    final BytesValueRLPInput in =
        encode(
            out -> {
              out.startList();
              out.writeIntScalar(30303); // only 1 field instead of the required 2 or 3
              out.endList();
            });

    assertThatThrownBy(() -> Endpoint.decodeStandalone(in))
        .isInstanceOf(PeerDiscoveryPacketDecodingException.class);
    // decodeStandalone's catch already skipped to the end of the malformed list before
    // rethrowing, so the enclosing list's cursor is still in sync for the next field.
    assertThat(in.readLongScalar()).isEqualTo(MARKER_VALUE);
  }

  @Test
  public void maybeDecodeStandalone_malformedFieldCount_returnsEmptyAndKeepsCursorInSync() {
    final BytesValueRLPInput in =
        encode(
            out -> {
              out.startList();
              out.writeIntScalar(30303);
              out.endList();
            });

    assertThat(Endpoint.maybeDecodeStandalone(in)).isEmpty();
    assertThat(in.readLongScalar()).isEqualTo(MARKER_VALUE);
  }

  @Test
  public void decodeStandalone_invalidPort_throwsAndKeepsCursorInSync() throws Exception {
    final InetAddress host = InetAddress.getByName("10.0.0.1");
    final BytesValueRLPInput in =
        encode(
            out -> {
              out.startList();
              out.writeInetAddress(host);
              out.writeIntScalar(70000); // outside the valid 1-65535 range
              out.endList();
            });

    assertThatThrownBy(() -> Endpoint.decodeStandalone(in))
        .isInstanceOf(IllegalPortException.class);
    assertThat(in.readLongScalar()).isEqualTo(MARKER_VALUE);
  }

  @Test
  public void maybeDecodeStandalone_invalidPort_returnsEmptyAndKeepsCursorInSync()
      throws Exception {
    final InetAddress host = InetAddress.getByName("10.0.0.1");
    final BytesValueRLPInput in =
        encode(
            out -> {
              out.startList();
              out.writeInetAddress(host);
              out.writeIntScalar(70000);
              out.endList();
            });

    assertThat(Endpoint.maybeDecodeStandalone(in)).isEmpty();
    assertThat(in.readLongScalar()).isEqualTo(MARKER_VALUE);
  }

  @Test
  public void decodeStandalone_malformedAddressBytes_throwsAndKeepsCursorInSync() {
    final BytesValueRLPInput in =
        encode(
            out -> {
              out.startList();
              // 5 raw bytes: neither a 4-byte IPv4 nor 16-byte IPv6 address.
              out.writeBytes(Bytes.fromHexString("0x0102030405"));
              out.writeIntScalar(30303);
              out.endList();
            });

    assertThatThrownBy(() -> Endpoint.decodeStandalone(in)).isInstanceOf(RLPException.class);
    assertThat(in.readLongScalar()).isEqualTo(MARKER_VALUE);
  }

  @Test
  public void maybeDecodeStandalone_malformedAddressBytes_returnsEmptyAndKeepsCursorInSync() {
    final BytesValueRLPInput in =
        encode(
            out -> {
              out.startList();
              out.writeBytes(Bytes.fromHexString("0x0102030405"));
              out.writeIntScalar(30303);
              out.endList();
            });

    assertThat(Endpoint.maybeDecodeStandalone(in)).isEmpty();
    assertThat(in.readLongScalar()).isEqualTo(MARKER_VALUE);
  }

  /**
   * Wraps the endpoint written by {@code endpointWriter} in an enclosing list followed by {@link
   * #MARKER_VALUE}, mirroring how a PING packet's "to"/"from" endpoint is followed by its
   * "expiration" field, then enters that enclosing list and returns the input positioned exactly
   * where {@code Endpoint.decodeStandalone}/{@code maybeDecodeStandalone} expect it.
   */
  private static BytesValueRLPInput encode(final Consumer<BytesValueRLPOutput> endpointWriter) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    endpointWriter.accept(out);
    out.writeLongScalar(MARKER_VALUE);
    out.endList();

    final BytesValueRLPInput in = new BytesValueRLPInput(out.encoded(), false);
    in.enterList();
    return in;
  }

  private static void writeEndpoint(
      final BytesValueRLPOutput out,
      final InetAddress host,
      final int udpPort,
      final Optional<Integer> tcpPort) {
    out.startList();
    out.writeInetAddress(host);
    out.writeIntScalar(udpPort);
    tcpPort.ifPresentOrElse(out::writeIntScalar, out::writeNull);
    out.endList();
  }
}
