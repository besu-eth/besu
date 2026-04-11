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
package org.hyperledger.besu.ethereum.p2p.rlpx.framing;

import static io.netty.buffer.ByteBufUtil.decodeHexDump;
import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.HandshakeSecrets;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

public class FramerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void shouldThrowExceptionWhenFramingMessageTooLong() {
    final byte[] aes = {
      0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa,
      0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2
    };
    final byte[] mac = {
      0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa,
      0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2, 0xa, 0x2
    };

    final byte[] byteArray = new byte[0xFFFFFF];
    new Random().nextBytes(byteArray);
    final MessageData ethMessage = new RawMessage(0x00, Bytes.wrap(byteArray));

    final HandshakeSecrets secrets = new HandshakeSecrets(aes, mac, mac);
    final Framer framer = new Framer(secrets);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> framer.frame(ethMessage, Unpooled.buffer()))
        .withMessageContaining("Message size in excess of maximum length.");
  }

  @Test
  public void shouldThrowExceptionWhenDeframingCompressedMessageTooLong() throws IOException {
    // This is a circular test.
    //
    // This test frames a too-large message; it then impersonates the sending end
    // by swapping the ingress and egress MACs and frames the plaintext messages.
    // The expected result is an exception on deframing not relating to encryption.
    //
    final JsonNode td = MAPPER.readTree(FramerTest.class.getResource("/peer1.json"));
    final HandshakeSecrets secrets = secretsFrom(td, false);
    final Framer framer = new Framer(secrets);
    framer.enableCompression();

    final byte[] byteArray = Snappy.compress(new byte[0x1000000]);

    final MessageData ethMessage = new RawMessage(0x00, Bytes.wrap(byteArray));
    final ByteBuf framedMessage = Unpooled.buffer();
    framer.frameMessage(ethMessage, framedMessage);

    final HandshakeSecrets deframeSecrets = secretsFrom(td, true);
    final Framer deframer = new Framer(deframeSecrets);
    deframer.enableCompression();

    assertThatExceptionOfType(FramingException.class)
        .isThrownBy(() -> deframer.deframe(framedMessage))
        .withMessageContaining("Message size 16777216 in excess of maximum length.");
  }

  @Test
  public void deframeOne() throws IOException {
    // Load test data.
    final JsonNode td = MAPPER.readTree(FramerTest.class.getResource("/peer2.json"));
    final HandshakeSecrets secrets = secretsFrom(td, false);

    final Framer framer = new Framer(secrets);
    final JsonNode m = td.get("messages").get(0);

    assertThatCode(() -> framer.deframe(wrappedBuffer(decodeHexDump(m.get("data").asText()))))
        .doesNotThrowAnyException();
  }

  @Test
  public void deframeExcessDataThrowsException() throws IOException {
    final JsonNode td = MAPPER.readTree(FramerTest.class.getResource("/peer2.json"));
    final HandshakeSecrets secrets = secretsFrom(td, false);
    final Framer framer = new Framer(secrets);

    // frame has 19 byte protocol header
    final ByteBuf badFrame =
        Unpooled.wrappedBuffer(
            decodeHexDump(
                "1457498af1e2ccd62d3ed4e013b1c78ab1a2790b7edbf77cf5fe903ae15beb024d264ee0942c5ac7c0f8fd1098f36a360cfcabdb958e9c6145b1ff6d41f937aa"));

    assertThatThrownBy(() -> framer.deframe(badFrame))
        .isInstanceOf(FramingException.class)
        .hasMessage("Expected at least 19 readable bytes while processing header, remaining: 13");
  }

  @Test
  public void deframeManyNoFragmentation() throws IOException {
    // Load test data.
    final JsonNode td = MAPPER.readTree(FramerTest.class.getResource("/peer1.json"));
    final HandshakeSecrets secrets = secretsFrom(td, false);

    final JsonNode messages = td.get("messages");
    final ByteBuf buf = buffer();

    messages.forEach(n -> buf.writeBytes(decodeHexDump(n.get("data").asText())));

    final Framer framer = new Framer(secrets);
    int i = 0;
    for (MessageData m = framer.deframe(buf); m != null; m = framer.deframe(buf)) {
      final int expectedFrameSize = messages.get(i++).get("frame_size").asInt();
      assertThat(expectedFrameSize).isEqualTo(m.getSize() + 1); // +1 for message id byte.
    }
    // All messages were processed.
    assertThat(i).isEqualTo(messages.size());
  }

  @Test
  public void deframeManyWithExtremeOneByteFragmentation() throws IOException {
    // Load test data.
    final JsonNode td = MAPPER.readTree(FramerTest.class.getResource("/peer1.json"));
    final HandshakeSecrets secrets = secretsFrom(td, false);

    final JsonNode messages = td.get("messages");

    //
    // TCP is a stream-based, fragmenting protocol; protocol messages can arrive chunked in smaller
    // packets
    // arbitrarily; however it is guaranteed that fragments will arrive in order.
    //
    // Here we test our framer is capable of reassembling fragments into protocol messages, by
    // simulating
    // an aggressive 1-byte fragmentation model.
    //
    final ByteBuf all = buffer();
    messages.forEach(n -> all.writeBytes(decodeHexDump(n.get("data").asText())));

    final Framer framer = new Framer(secrets);

    int i = 0;
    final ByteBuf in = buffer();
    while (all.isReadable()) {
      in.writeByte(all.readByte());
      final MessageData msg = framer.deframe(in);
      if (msg != null) {
        final int expectedFrameSize = messages.get(i++).get("frame_size").asInt();
        assertThat(expectedFrameSize).isEqualTo(msg.getSize() + 1); // +1 for message id byte.
        assertThat(in.readableBytes()).isZero();
      }
    }
    // All messages were processed.
    assertThat(i).isEqualTo(messages.size());
  }

  @Test
  public void frameMessage() throws IOException {
    // This is a circular test.
    //
    // This test decrypts all messages in the test vectors; it then impersonates the sending end
    // by swapping the ingress and egress MACs and frames the plaintext messages.
    // We then verify if the resulting ciphertexts are equal to our test vectors.
    //
    final JsonNode td = MAPPER.readTree(FramerTest.class.getResource("/peer1.json"));
    HandshakeSecrets secrets = secretsFrom(td, false);
    Framer framer = new Framer(secrets);

    final JsonNode messages = td.get("messages");
    final List<MessageData> decrypted =
        stream(messages.spliterator(), false)
            .map(n -> decodeHexDump(n.get("data").asText()))
            .map(Unpooled::wrappedBuffer)
            .map(framer::deframe)
            .collect(toList());

    secrets = secretsFrom(td, true);
    framer = new Framer(secrets);

    for (int i = 0; i < decrypted.size(); i++) {
      final ByteBuf b = Unpooled.buffer();
      framer.frame(decrypted.get(i), b);
      final byte[] enc = new byte[b.readableBytes()];
      b.readBytes(enc);
      final byte[] expected = decodeHexDump(messages.get(i).get("data").asText());
      assertThat(expected).isEqualTo(enc);
    }
  }

  @Test
  public void downgradesToUncompressed() {
    final HandshakeSecrets secrets =
        new HandshakeSecrets(
            Bytes.fromHexString(
                    "0x75b3ee95adff0c529a05efd7612aa1dbe5057eb9facdde0dfc837ad143da1d43")
                .toArray(),
            Bytes.fromHexString(
                    "0x030dfd1566f4800c4842c177f7d476b64ae2b99a2aa0ab5600aa2f41a8710575")
                .toArray(),
            Bytes.fromHexString(
                    "0xc9d3385b1588a5969cba312f8c29bedb4cb9d56ec0cf825436addc1ec644f1d6")
                .toArray());
    final Framer receivingFramer = new Framer(secrets);
    final Framer sendingFramer = new Framer(secrets);

    // Write a disconnect message with compression disabled.
    final ByteBuf out = Unpooled.buffer();
    sendingFramer.frame(DisconnectMessage.create(DisconnectReason.TIMEOUT), out);

    // Then read it with compression enabled.
    receivingFramer.enableCompression();
    assertThat(receivingFramer.deframe(out)).isNotNull();
    assertThat(receivingFramer.isCompressionEnabled()).isFalse();
  }

  @Test
  public void compressionWorks() {
    final HandshakeSecrets secrets =
        new HandshakeSecrets(
            Bytes.fromHexString(
                    "0x75b3ee95adff0c529a05efd7612aa1dbe5057eb9facdde0dfc837ad143da1d43")
                .toArray(),
            Bytes.fromHexString(
                    "0x030dfd1566f4800c4842c177f7d476b64ae2b99a2aa0ab5600aa2f41a8710575")
                .toArray(),
            Bytes.fromHexString(
                    "0xc9d3385b1588a5969cba312f8c29bedb4cb9d56ec0cf825436addc1ec644f1d6")
                .toArray());
    final Framer receivingFramer = new Framer(secrets);
    final Framer sendingFramer = new Framer(secrets);

    // Write a disconnect message with compression enabled.
    sendingFramer.enableCompression();
    final ByteBuf out = Unpooled.buffer();
    sendingFramer.frame(DisconnectMessage.create(DisconnectReason.TIMEOUT), out);

    // Then read it with compression enabled.
    receivingFramer.enableCompression();
    assertThat(receivingFramer.deframe(out)).isNotNull();
    assertThat(receivingFramer.isCompressionEnabled()).isTrue();
    assertThat(receivingFramer.isCompressionSuccessful()).isTrue();
  }

  @Test
  public void firstMessageDecompressesEagerlySubsequentMessagesDeferred() {
    final HandshakeSecrets sendSecrets =
        new HandshakeSecrets(
            Bytes.fromHexString(
                    "0x75b3ee95adff0c529a05efd7612aa1dbe5057eb9facdde0dfc837ad143da1d43")
                .toArray(),
            Bytes.fromHexString(
                    "0x030dfd1566f4800c4842c177f7d476b64ae2b99a2aa0ab5600aa2f41a8710575")
                .toArray(),
            Bytes.fromHexString(
                    "0xc9d3385b1588a5969cba312f8c29bedb4cb9d56ec0cf825436addc1ec644f1d6")
                .toArray());
    final HandshakeSecrets recvSecrets =
        new HandshakeSecrets(
            Bytes.fromHexString(
                    "0x75b3ee95adff0c529a05efd7612aa1dbe5057eb9facdde0dfc837ad143da1d43")
                .toArray(),
            Bytes.fromHexString(
                    "0x030dfd1566f4800c4842c177f7d476b64ae2b99a2aa0ab5600aa2f41a8710575")
                .toArray(),
            Bytes.fromHexString(
                    "0xc9d3385b1588a5969cba312f8c29bedb4cb9d56ec0cf825436addc1ec644f1d6")
                .toArray());
    final Framer sendingFramer = new Framer(sendSecrets);
    final Framer receivingFramer = new Framer(recvSecrets);
    sendingFramer.enableCompression();
    receivingFramer.enableCompression();

    final Bytes payload1 = DisconnectMessage.create(DisconnectReason.TIMEOUT).getData();
    final Bytes payload2 = DisconnectMessage.create(DisconnectReason.BREACH_OF_PROTOCOL).getData();

    // Frame two messages
    final ByteBuf out1 = Unpooled.buffer();
    sendingFramer.frame(new RawMessage(0x01, payload1), out1);
    final ByteBuf out2 = Unpooled.buffer();
    sendingFramer.frame(new RawMessage(0x01, payload2), out2);

    // First message: eagerly decompressed (compressionSuccessful was false)
    final MessageData msg1 = receivingFramer.deframe(out1);
    assertThat(msg1).isNotNull();
    final RawMessage raw1 = (RawMessage) msg1;
    assertThat(raw1.getCompressedData()).isNull();
    assertThat(raw1.getData()).isEqualTo(payload1);
    assertThat(receivingFramer.isCompressionSuccessful()).isTrue();

    // Second message: deferred decompression (compressionSuccessful was true)
    final MessageData msg2 = receivingFramer.deframe(out2);
    assertThat(msg2).isNotNull();
    final RawMessage raw2 = (RawMessage) msg2;
    assertThat(raw2.getCompressedData()).isNotNull();
    // getData() triggers lazy decompression
    assertThat(raw2.getData()).isEqualTo(payload2);
    // Compressed data released after decompression
    assertThat(raw2.getCompressedData()).isNull();
  }

  private HandshakeSecrets secretsFrom(final JsonNode td, final boolean swap) {
    final byte[] aes = decodeHexDump(td.get("aes_secret").asText());
    final byte[] mac = decodeHexDump(td.get("mac_secret").asText());

    final byte[] e1 = decodeHexDump(td.get("egress_gen").get(0).asText());
    final byte[] e2 = decodeHexDump(td.get("egress_gen").get(1).asText());
    final byte[] i1 = decodeHexDump(td.get("ingress_gen").get(0).asText());
    final byte[] i2 = decodeHexDump(td.get("ingress_gen").get(1).asText());

    // 3rd parameter (token) is irrelevant.
    final HandshakeSecrets secrets = new HandshakeSecrets(aes, mac, mac);

    if (!swap) {
      secrets.updateEgress(e1).updateEgress(e2).updateIngress(i1).updateIngress(i2);
    } else {
      secrets.updateIngress(e1).updateIngress(e2).updateEgress(i1).updateEgress(i2);
    }

    return secrets;
  }
}
