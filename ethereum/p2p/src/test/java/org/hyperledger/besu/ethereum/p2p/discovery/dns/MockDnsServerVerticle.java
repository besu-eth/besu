/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.p2p.discovery.dns;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;
import com.google.common.io.Resources;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Mock DNS server verticle. */
public class MockDnsServerVerticle extends AbstractVerticle {
  private static final Logger LOG = LoggerFactory.getLogger(MockDnsServerVerticle.class);
  private static final int MAX_CHARACTER_STRING = 255;
  private final Map<String, List<String>> txtRecords = new HashMap<>();
  private int dnsPort;

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    final DatagramSocket datagramSocket = vertx.createDatagramSocket(new DatagramSocketOptions());
    datagramSocket.handler(packet -> handleDatagramPacket(datagramSocket, packet));

    final String dnsEntriesJsonPath =
        Path.of(Resources.getResource("discovery/dns/dns-records.json").toURI()).toString();
    LOG.debug("Reading DNS entries from: {}", dnsEntriesJsonPath);
    vertx
        .fileSystem()
        .readFile(dnsEntriesJsonPath)
        .compose(
            buffer -> {
              final JsonObject dnsEntries = new JsonObject(buffer.toString());
              final Map<String, Object> jsonMap = dnsEntries.getMap();
              jsonMap.forEach((key, value) -> addTxtRecord(key, value.toString()));

              // start the server
              return datagramSocket.listen(0, "127.0.0.1");
            })
        .onComplete(
            res -> {
              if (res.succeeded()) {
                LOG.info("Mock Dns Server is now listening {}", res.result().localAddress());
                dnsPort = res.result().localAddress().port();
                startPromise.complete();
              } else {
                startPromise.fail(res.cause());
              }
            });
  }

  public void addTxtRecord(final String key, final String value) {
    final List<String> records = new ArrayList<>();
    records.add(value);
    txtRecords.put(key, records);
  }

  /** Publishes an additional TXT record at a name that already has one. */
  public void appendTxtRecord(final String key, final String value) {
    txtRecords.computeIfAbsent(key, unused -> new ArrayList<>()).add(value);
  }

  @Override
  public void stop() {
    LOG.info("Stopping Mock DNS Server");
  }

  private void handleDatagramPacket(final DatagramSocket socket, final DatagramPacket packet) {
    LOG.debug("Packet Received");
    Buffer data = packet.data();
    final short queryId = getQueryId(data);
    final String queryName = extractQueryName(data.getBytes());

    final Buffer response;
    if (txtRecords.containsKey(queryName)) {
      LOG.debug("Query name found {}", queryName);
      response = createTXTResponse(queryId, queryName, txtRecords.get(queryName));
    } else {
      LOG.debug("Query name not found: {}", queryName);
      response = createErrorResponse(queryId, queryName);
    }

    socket.send(response, packet.sender().port(), packet.sender().host());
  }

  private String extractQueryName(final byte[] buffer) {
    StringBuilder queryName = new StringBuilder();
    int index = 12; // Skip the DNS header

    while (index < buffer.length) {
      int labelLength = buffer[index] & 0xFF;

      if (labelLength == 0) {
        break;
      }

      index++;

      for (int i = 0; i < labelLength; i++) {
        char c = (char) (buffer[index + i] & 0xFF);
        queryName.append(c);
      }

      index += labelLength;

      if (index < buffer.length && buffer[index] != 0) {
        queryName.append(".");
      }
    }

    return queryName.toString();
  }

  private Buffer createTXTResponse(
      final short queryId, final String queryName, final List<String> records) {
    final Buffer buffer = Buffer.buffer();

    // Write DNS header
    buffer.appendShort(queryId); // Query Identifier
    buffer.appendShort((short) 0x8180); // Flags (Standard query response, No error)
    buffer.appendShort((short) 1); // Questions count
    buffer.appendShort((short) records.size()); // Answers count
    buffer.appendShort((short) 0); // Authority RRs count
    buffer.appendShort((short) 0); // Additional RRs count

    // Write query name
    final Iterable<String> queryLabels = Splitter.on(".").split(queryName);
    for (String label : queryLabels) {
      buffer.appendByte((byte) label.length());
      buffer.appendString(label);
    }
    buffer.appendByte((byte) 0); // End of query name

    // Write query type and class
    buffer.appendShort((short) 16); // Type (TXT)
    buffer.appendShort((short) 1); // Class (IN)

    for (final String txtRecord : records) {
      // Compression pointer back to the question name at offset 0x0C (RFC 1035 4.1.4), rather than
      // repeating the (lowercased) labels. This isn't a decoder strictness issue -- the previous
      // repeated-labels form decoded fine -- it's that Vert.x 5's DnsClientImpl (rewritten to
      // delegate to Netty's DnsNameResolver) added a case-sensitive equality check between an
      // answer's owner name and the query name, silently dropping any record that fails it;
      // Vert.x 4.5.x had no such check. Lowercasing the labels here made the answer name diverge
      // from the (non-lowercased) query name above, so the record was filtered out before this
      // test ever saw it. A real EIP-1459 server whose response doesn't echo the query bytes
      // verbatim would hit the same silent-drop in production -- see DNSResolver#resolveTxtStrings.
      buffer.appendShort((short) 0xC00C);

      buffer.appendShort((short) 16); // TXT record type
      buffer.appendShort((short) 1); // Class (IN)
      buffer.appendInt(60); // TTL (60 seconds)

      // RFC 1035 caps a <character-string> at 255 bytes, so a real server splits longer content
      // into several of them within one RDATA. A single length byte would overflow past 255.
      final byte[] txtBytes = txtRecord.getBytes(UTF_8);
      final int chunks =
          Math.max(1, (txtBytes.length + MAX_CHARACTER_STRING - 1) / MAX_CHARACTER_STRING);
      buffer.appendShort((short) (txtBytes.length + chunks)); // Data length
      for (int offset = 0; offset < txtBytes.length; offset += MAX_CHARACTER_STRING) {
        final int length = Math.min(MAX_CHARACTER_STRING, txtBytes.length - offset);
        buffer.appendByte((byte) length);
        buffer.appendBytes(txtBytes, offset, length);
      }
      if (txtBytes.length == 0) {
        buffer.appendByte((byte) 0);
      }
    }

    return buffer;
  }

  private Buffer createErrorResponse(final short queryId, final String queryName) {
    Buffer buffer = Buffer.buffer();

    // Write DNS header
    buffer.appendShort(queryId); // Query Identifier
    buffer.appendShort((short) 0x8183); // Flags (Standard query response, NXDOMAIN error)
    buffer.appendShort((short) 1); // Questions count
    buffer.appendShort((short) 0); // Answers count
    buffer.appendShort((short) 0); // Authority RRs count
    buffer.appendShort((short) 0); // Additional RRs count

    // Write query name
    for (String label : Splitter.on(".").split(queryName)) {
      buffer.appendByte((byte) label.length());
      buffer.appendString(label);
    }
    buffer.appendByte((byte) 0); // End of query name

    // Write query type and class
    buffer.appendShort((short) 16); // Type (TXT)
    buffer.appendShort((short) 1); // Class (IN)

    return buffer;
  }

  private short getQueryId(final Buffer queryData) {
    return (short) ((queryData.getByte(0) & 0xff) << 8 | (queryData.getByte(1) & 0xff));
  }

  /**
   * Mock server local port
   *
   * @return server port
   */
  public int port() {
    return dnsPort;
  }
}
