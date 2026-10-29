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
package org.hyperledger.besu.ethereum.eth.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.EthProtocolVersion.V68;
import static org.hyperledger.besu.ethereum.eth.encoding.TransactionAnnouncementDecoder.getDecoder;
import static org.hyperledger.besu.ethereum.eth.encoding.TransactionAnnouncementEncoder.getEncoder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAnnouncement;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TransactionAnnouncementEncoderDecoderTest {
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final int maxAnnouncementsPerMessage =
      EthProtocolConfiguration.DEFAULT_MAX_ANNOUNCEMENTS_PER_MESSAGE;

  @Test
  void shouldEncodeTransactionsCorrectly() {

    final String expected =
        "0xf86d83000102c3010203f863a00000000000000000000000000000000000000000000000000000000000000001a00000000000000000000000000000000000000000000000000000000000000002a00000000000000000000000000000000000000000000000000000000000000003";
    final List<Hash> hashes =
        List.of(
            Hash.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000001"),
            Hash.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000002"),
            Hash.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000003"));
    final List<Integer> sizes = List.of(1, 2, 3);
    final List<TransactionType> types =
        List.of(TransactionType.FRONTIER, TransactionType.ACCESS_LIST, TransactionType.EIP1559);

    final Bytes bytes = TransactionAnnouncementEncoder.encodeForEth68(types, sizes, hashes);
    assertThat(expected).isEqualTo(bytes.toHexString());
  }

  @Test
  void shouldDecodeBytesCorrectly() {
    /*
     * [
     * "0x0000102"]
     * ["0x01","0x02","0x03"],
     * ["0x0000000000000000000000000000000000000000000000000000000000000001",
     *  "0x0000000000000000000000000000000000000000000000000000000000000002",
     *  "0x0000000000000000000000000000000000000000000000000000000000000003"]
     * ]
     */

    final Bytes bytes =
        Bytes.fromHexString(
            "0xf86d83000102c3010203f863a00000000000000000000000000000000000000000000000000000000000000001a00000000000000000000000000000000000000000000000000000000000000002a00000000000000000000000000000000000000000000000000000000000000003");

    final List<TransactionAnnouncement> announcementList =
        getDecoder(V68.getCapability()).decode(RLP.input(bytes), maxAnnouncementsPerMessage);

    final TransactionAnnouncement frontier = announcementList.getFirst();
    assertThat(frontier.hash())
        .isEqualTo(
            Hash.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000001"));
    assertThat(frontier.type()).isEqualTo(TransactionType.FRONTIER);
    assertThat(frontier.size()).isEqualTo(1L);

    final TransactionAnnouncement accessList = announcementList.get(1);
    assertThat(accessList.hash())
        .isEqualTo(
            Hash.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000002"));
    assertThat(accessList.type()).isEqualTo(TransactionType.ACCESS_LIST);
    assertThat(accessList.size()).isEqualTo(2L);

    final TransactionAnnouncement eip1559 = announcementList.get(2);
    assertThat(eip1559.hash())
        .isEqualTo(
            Hash.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000003"));
    assertThat(eip1559.type()).isEqualTo(TransactionType.EIP1559);
    assertThat(eip1559.size()).isEqualTo(3L);
  }

  @Test
  void shouldEncodeAndDecodeTransactionAnnouncement_Eth68() {
    final Transaction t1 = generator.transaction(TransactionType.FRONTIER);
    final Transaction t2 = generator.transaction(TransactionType.ACCESS_LIST);
    final Transaction t3 = generator.transaction(TransactionType.EIP1559);

    final List<Transaction> list = List.of(t1, t2, t3);
    final Bytes bytes = getEncoder(V68.getCapability()).encode(list);

    final List<TransactionAnnouncement> announcementList =
        getDecoder(V68.getCapability()).decode(RLP.input(bytes), maxAnnouncementsPerMessage);

    assertThat(announcementList).hasSameSizeAs(list);

    for (final Transaction transaction : list) {
      final TransactionAnnouncement announcement = announcementList.get(list.indexOf(transaction));
      assertThat(announcement.hash()).isEqualTo(transaction.getHash());
      assertThat(announcement.type()).isEqualTo(transaction.getType());
      assertThat(announcement.size()).isEqualTo(transaction.getSizeForAnnouncement());
    }
  }

  @Test
  void shouldThrowInvalidArgumentExceptionWhenCreatingListsWithDifferentSizes() {
    assertThatThrownBy(
            () -> TransactionAnnouncement.create(new ArrayList<>(), List.of(1L), new ArrayList<>()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Hashes, sizes and types must have the same number of elements");
  }

  @Test
  void shouldThrowInvalidArgumentExceptionWhenEncodingListsWithDifferentSizes() {
    assertThatThrownBy(
            () ->
                TransactionAnnouncementEncoder.encodeForEth68(
                    new ArrayList<>(), List.of(1), new ArrayList<>()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Hashes, sizes and types must have the same number of elements");
  }

  @Test
  @SuppressWarnings("UnusedVariable")
  void shouldThrowRLPExceptionWhenDecodingListsWithDifferentSizes() {

    // ["0x000102",[],["0x881699519a25b0e32db9b1ba9981f3fbec93fbc0726c3e096af89e5ada2b1351"]]
    final Bytes invalidMessageBytes =
        Bytes.fromHexString(
            "0xe783000102c0e1a0881699519a25b0e32db9b1ba9981f3fbec93fbc0726c3e096af89e5ada2b1351");

    assertThatThrownBy(
            () ->
                TransactionAnnouncementDecoder.getDecoder(V68.getCapability())
                    .decode(RLP.input(invalidMessageBytes), maxAnnouncementsPerMessage))
        .isInstanceOf(RLPException.class)
        .hasMessage("Hashes, sizes and types must have the same number of elements");
  }

  @Test
  void shouldThrowRLPExceptionWhenTypeIsInvalid() {
    final Bytes invalidMessageBytes =
        Bytes.fromHexString(
            // ["0x07",["0x00000002"],["0x881699519a25b0e32db9b1ba9981f3fbec93fbc0726c3e096af89e5ada2b1351"]]
            "0xe907c58400000002e1a0881699519a25b0e32db9b1ba9981f3fbec93fbc0726c3e096af89e5ada2b1351");

    assertThatThrownBy(
            () ->
                TransactionAnnouncementDecoder.getDecoder(V68.getCapability())
                    .decode(RLP.input(invalidMessageBytes), maxAnnouncementsPerMessage))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("Invalid transaction type 0x07");
  }

  @Test
  void shouldThrowRLPExceptionWhenSizeSizeGreaterThanFourBytes() {
    final Bytes invalidMessageBytes =
        Bytes.fromHexString(
            // ["0x02",["0xffffffff01"],["0x881699519a25b0e32db9b1ba9981f3fbec93fbc0726c3e096af89e5ada2b1351"]]
            "0xea02c685ffffffff00e1a0881699519a25b0e32db9b1ba9981f3fbec93fbc0726c3e096af89e5ada2b1351");

    assertThatThrownBy(
            () ->
                TransactionAnnouncementDecoder.getDecoder(V68.getCapability())
                    .decode(RLP.input(invalidMessageBytes), maxAnnouncementsPerMessage))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining(
            "Cannot read a unsigned int scalar, expecting a maximum of 4 bytes but current element is 5 bytes long");
  }

  @Test
  void shouldThrowNullPointerIfArgumentsAreNull() {
    final Hash hash = Hash.hash(Bytes.random(32));

    assertThatThrownBy(() -> new TransactionAnnouncement(null, TransactionType.EIP1559, 0L))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Hash cannot be null");

    assertThatThrownBy(() -> new TransactionAnnouncement(hash, null, 0L))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Type cannot be null");

    assertThatThrownBy(() -> new TransactionAnnouncement(hash, TransactionType.EIP1559, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Size cannot be null");

    assertThatThrownBy(() -> new TransactionAnnouncement(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Transaction cannot be null");
  }

  @Test
  void shouldRejectMoreTypesThanMaxAnnouncements() {
    final int maxAnnouncements = 2;
    final Bytes message = eth68Announcements(3, 3, 3);

    assertThatThrownBy(
            () -> getDecoder(V68.getCapability()).decode(RLP.input(message), maxAnnouncements))
        .isInstanceOf(RLPException.class)
        .hasMessage("Too many transaction announcements: received 3, while max is 2");
  }

  @Test
  void shouldDecodeExactlyMaxAnnouncements() {
    final int maxAnnouncements = 3;
    final Bytes message = eth68Announcements(3, 3, 3);

    assertThat(getDecoder(V68.getCapability()).decode(RLP.input(message), maxAnnouncements))
        .hasSize(3);
  }

  @Test
  void shouldStopReadingSizesBeyondTheNumberOfTypes() {
    // one type but many sizes: the sizes list must not be decoded past the number of types
    final Bytes message = eth68Announcements(1, 1000, 1);

    assertThatThrownBy(
            () ->
                getDecoder(V68.getCapability())
                    .decode(RLP.input(message), maxAnnouncementsPerMessage))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceeds the maximum permitted size of 1");
  }

  @Test
  void shouldStopReadingHashesBeyondTheNumberOfTypes() {
    final Bytes message = eth68Announcements(1, 1, 1000);

    assertThatThrownBy(
            () ->
                getDecoder(V68.getCapability())
                    .decode(RLP.input(message), maxAnnouncementsPerMessage))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceeds the maximum permitted size of 1");
  }

  @Test
  void shouldRejectMoreThanThreeTopLevelElements() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(Bytes.of(0x02));
    out.writeList(List.of(1L), (size, rlp) -> rlp.writeLongScalar(size));
    out.writeList(List.of(Hash.ZERO), (hash, rlp) -> rlp.writeBytes(hash.getBytes()));
    out.writeBytes(Bytes.of(0x00));
    out.endList();
    final Bytes message = out.encoded();

    assertThatThrownBy(
            () ->
                getDecoder(V68.getCapability())
                    .decode(RLP.input(message), maxAnnouncementsPerMessage))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceeds the maximum permitted size of 3");
  }

  /** An eth/68 announcement message with independently sized type, size and hash lists. */
  private static Bytes eth68Announcements(
      final int typeCount, final int sizeCount, final int hashCount) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(Bytes.wrap(new byte[typeCount])); // all FRONTIER (0x00)
    out.startList();
    for (int i = 0; i < sizeCount; i++) {
      out.writeLongScalar(1);
    }
    out.endList();
    out.startList();
    for (int i = 0; i < hashCount; i++) {
      out.writeBytes(Hash.hash(Bytes.ofUnsignedInt(i)).getBytes());
    }
    out.endList();
    out.endList();
    return out.encoded();
  }

  @Test
  void worstCaseMessageNeverExceedsTheDerivedMaxMessageSize() {
    // covers every count where an RLP length prefix grows (payloads of 56 and 256 bytes, ...)
    for (int count = 0; count <= 2_000; count++) {
      assertThat(worstCaseAnnouncements(count).size())
          .as("%d worst-case announcements", count)
          .isLessThanOrEqualTo(TransactionAnnouncementEncoder.maxMessageSize(count));
    }
  }

  @Test
  void defaultMaxMessageSizeAdmitsAFullWorstCaseMessage() {
    final int maxAnnouncements = EthProtocolConfiguration.DEFAULT_MAX_ANNOUNCEMENTS_PER_MESSAGE;
    final int maxMessageSize =
        EthProtocolConfiguration.DEFAULT.getMaxNewPooledTransactionHashesMessageSize();

    assertThat(maxMessageSize)
        .isEqualTo(
            TransactionAnnouncementEncoder.MAX_ENCODED_MESSAGE_OVERHEAD
                + maxAnnouncements * TransactionAnnouncementEncoder.MAX_ENCODED_ANNOUNCEMENT_SIZE);
    assertThat(worstCaseAnnouncements(maxAnnouncements).size()).isLessThanOrEqualTo(maxMessageSize);
  }

  @Test
  void maxMessageSizeDoesNotOverflow() {
    assertThat(TransactionAnnouncementEncoder.maxMessageSize(Integer.MAX_VALUE))
        .isEqualTo(Integer.MAX_VALUE);
  }

  /** Announcements with the largest possible encoded size: maximum-width size scalars. */
  private static Bytes worstCaseAnnouncements(final int count) {
    final byte[] types = new byte[count];
    final List<Integer> sizes = new ArrayList<>(count);
    final List<Hash> hashes = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      types[i] = TransactionType.DELEGATE_CODE.getEthSerializedType();
      sizes.add(Integer.MAX_VALUE);
      hashes.add(Hash.ZERO);
    }
    return TransactionAnnouncementEncoder.encodeForEth68(types, sizes, hashes);
  }

  @ParameterizedTest
  @EnumSource(EthProtocolVersion.class)
  void maxMessageSizeCoversAFullWorstCaseMessageInEverySupportedVersion(
      final EthProtocolVersion version) {
    final Capability capability = Capability.create(EthProtocol.NAME, version.getVersion());
    final int maxAnnouncements = EthProtocolConfiguration.DEFAULT_MAX_ANNOUNCEMENTS_PER_MESSAGE;
    final int maxMessageSize = TransactionAnnouncementEncoder.maxMessageSize(maxAnnouncements);

    final Bytes encoded =
        getEncoder(capability)
            .encode(Collections.nCopies(maxAnnouncements, worstCaseAnnouncedTransaction()));

    assertThat(encoded.size())
        .withFailMessage(
            "A NewPooledTransactionHashes message with %d worst-case announcements encodes to %d"
                + " bytes for eth/%d, more than the %d bytes TransactionAnnouncementEncoder"
                + ".maxMessageSize allows, so peers on eth/%d would be disconnected for sending a"
                + " valid message. If the message format changed, update"
                + " MAX_ENCODED_ANNOUNCEMENT_SIZE and MAX_ENCODED_MESSAGE_OVERHEAD in"
                + " TransactionAnnouncementEncoder to cover it.",
            maxAnnouncements,
            encoded.size(),
            version.getVersion(),
            maxMessageSize,
            version.getVersion())
        .isLessThanOrEqualTo(maxMessageSize);
    assertThat(getDecoder(capability).decode(RLP.input(encoded), maxAnnouncements))
        .hasSize(maxAnnouncements);
  }

  /**
   * A transaction whose announcement has the largest possible encoding: a size that needs the full
   * 4-byte scalar. Mocked, since a real transaction of that size would be gigabytes.
   */
  private static Transaction worstCaseAnnouncedTransaction() {
    final Transaction transaction = mock(Transaction.class);
    when(transaction.getType()).thenReturn(TransactionType.DELEGATE_CODE);
    when(transaction.getSizeForAnnouncement()).thenReturn(Integer.MAX_VALUE);
    when(transaction.getHash()).thenReturn(Hash.hash(Bytes.of(1)));
    return transaction;
  }
}
