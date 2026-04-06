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
package org.hyperledger.besu.ethereum.eth.messages;

import static java.math.BigInteger.TWO;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hyperledger.besu.ethereum.core.Transaction.REPLAY_PROTECTED_V_BASE;
import static org.hyperledger.besu.ethereum.core.Transaction.REPLAY_PROTECTED_V_MIN;
import static org.hyperledger.besu.ethereum.core.Transaction.REPLAY_UNPROTECTED_V_BASE;
import static org.hyperledger.besu.ethereum.core.Transaction.REPLAY_UNPROTECTED_V_BASE_PLUS_1;
import static org.hyperledger.besu.ethereum.eth.core.Utils.serializeReceiptsList;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class MessageWrapperTest {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final SimpleModule module = new SimpleModule();

  static {
    module.addDeserializer(Transaction.class, new TransactionDeserializer());
    objectMapper.registerModule(module);
  }

  @Test
  public void GetBlockHeaders() throws IOException {
    final var testJson = parseTestFile("GetBlockHeadersPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final GetBlockHeadersMessage getBlockHeadersMessage =
        GetBlockHeadersMessage.create(
            Hash.fromHexString(
                "0x00000000000000000000000000000000000000000000000000000000deadc0de"),
            5,
            5,
            false);
    final Bytes actual = getBlockHeadersMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void GetBlockHeaders1() throws IOException {
    final var testJson = parseTestFile("GetBlockHeadersPacket66-1.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final GetBlockHeadersMessage getBlockHeadersMessage =
        GetBlockHeadersMessage.create(9999, 5, 5, false);
    final Bytes actual = getBlockHeadersMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void BlockHeaders() throws IOException {
    final var testJson = parseTestFile("BlockHeadersPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final BlockHeadersMessage blockHeadersMessage =
        BlockHeadersMessage.create(
            Arrays.asList(
                objectMapper.treeToValue(
                    testJson.get("data").get("BlockHeadersPacket"), TestBlockHeader[].class)));
    final Bytes actual = blockHeadersMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void GetBlockBodies() throws IOException {
    final var testJson = parseTestFile("GetBlockBodiesPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final GetBlockBodiesMessage getBlockBodiesMessage =
        GetBlockBodiesMessage.create(
            Stream.of(
                    "0x00000000000000000000000000000000000000000000000000000000deadc0de",
                    "0x00000000000000000000000000000000000000000000000000000000feedbeef")
                .map(Hash::fromHexString)
                .collect(toUnmodifiableList()));
    final Bytes actual = getBlockBodiesMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void BlockBodies() throws IOException {
    final var testJson = parseTestFile("BlockBodiesPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final BlockBodiesMessage blockBodiesMessage =
        BlockBodiesMessage.create(
            Arrays.asList(
                objectMapper.treeToValue(
                    testJson.get("data").get("BlockBodiesPacket"), TestBlockBody[].class)));
    final Bytes actual = blockBodiesMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void GetNodeData() throws IOException {
    final var testJson = parseTestFile("GetNodeDataPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final GetNodeDataMessage getNodeDataMessage =
        GetNodeDataMessage.create(
            Stream.of(
                    "0x00000000000000000000000000000000000000000000000000000000deadc0de",
                    "0x00000000000000000000000000000000000000000000000000000000feedbeef")
                .map(Hash::fromHexString)
                .collect(toUnmodifiableList()));
    final Bytes actual = getNodeDataMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void NodeData() throws IOException {
    final var testJson = parseTestFile("NodeDataPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final NodeDataMessage nodeDataMessage =
        NodeDataMessage.create(
            Stream.of("0xdeadc0de", "0xfeedbeef")
                .map(Bytes::fromHexString)
                .collect(toUnmodifiableList()));
    final Bytes actual = nodeDataMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void GetReceipts() throws IOException {
    final var testJson = parseTestFile("GetReceiptsPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final GetReceiptsMessage getReceiptsMessage =
        GetReceiptsMessage.create(
            Stream.of(
                    "0x00000000000000000000000000000000000000000000000000000000deadc0de",
                    "0x00000000000000000000000000000000000000000000000000000000feedbeef")
                .map(Hash::fromHexString)
                .collect(toUnmodifiableList()));
    final Bytes actual = getReceiptsMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void Receipts() throws IOException {
    final var testJson = parseTestFile("ReceiptsPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final ReceiptsMessage receiptsMessage =
        ReceiptsMessage.createUnsafe(
            serializeReceiptsList(
                singletonList(
                    singletonList(
                        new TransactionReceipt(
                            TransactionType.FRONTIER,
                            0,
                            1,
                            singletonList(
                                new LogWithMetadata(
                                    0,
                                    0,
                                    Hash.ZERO,
                                    0L,
                                    Hash.ZERO,
                                    0,
                                    Address.fromHexString("0x11"),
                                    Bytes.fromHexString("0x0100ff"),
                                    Stream.of(
                                            "0x000000000000000000000000000000000000000000000000000000000000dead",
                                            "0x000000000000000000000000000000000000000000000000000000000000beef")
                                        .map(LogTopic::fromHexString)
                                        .collect(toUnmodifiableList()),
                                    false)),
                            LogsBloomFilter.fromHexString(
                                "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
                            Optional.empty()))),
                TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));
    final Bytes actual = receiptsMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void GetPooledTransactions() throws IOException {
    final var testJson = parseTestFile("GetPooledTransactionsPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final GetPooledTransactionsMessage getPooledTransactionsMessage =
        GetPooledTransactionsMessage.create(
            Stream.of(
                    "0x00000000000000000000000000000000000000000000000000000000deadc0de",
                    "0x00000000000000000000000000000000000000000000000000000000feedbeef")
                .map(Hash::fromHexString)
                .collect(toUnmodifiableList()));
    final Bytes actual =
        getPooledTransactionsMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void PooledTransactions() throws IOException {
    final var testJson = parseTestFile("PooledTransactionsPacket66.json");
    final Bytes expected = Bytes.fromHexString(testJson.get("rlp").asText());
    final PooledTransactionsMessage pooledTransactionsMessage =
        PooledTransactionsMessage.create(
            Arrays.asList(
                objectMapper.treeToValue(
                    testJson.get("data").get("PooledTransactionsPacket"), Transaction[].class)));
    final Bytes actual =
        pooledTransactionsMessage.wrapMessageData(BigInteger.valueOf(1111)).getData();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void readFromExpectsListWrappingBodyFields() {
    // BodiesMessages broadcast bodies in this format
    // [[txs],[ommers],[withdrawals]]
    // 0xc3c0c0c0
    final BytesValueRLPInput bytesValueRLPInput =
        new BytesValueRLPInput(Bytes.fromHexString("0xc3c0c0c0"), false);
    final BlockBody blockBodyDecodefromRLP =
        BlockBody.readWrappedBodyFrom(bytesValueRLPInput, new MainnetBlockHeaderFunctions());

    assertThat(blockBodyDecodefromRLP)
        .isEqualTo(
            new BlockBody(
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.of(Collections.emptyList())));
  }

  @Test
  public void readBodyFieldsExpectsNoListWrappingBodyFields() {
    // rlps of blocks contains block data in this format
    // [[blockheader],[txs],[ommers],[withdrawals]]
    // 0xc4c0c0c0c0
    final BytesValueRLPInput bytesValueRLPInput =
        new BytesValueRLPInput(Bytes.fromHexString("0xc4c0c0c0c0"), false);
    // Enters the initial
    bytesValueRLPInput.enterList();
    // skips block header list
    bytesValueRLPInput.enterList();
    bytesValueRLPInput.leaveList();

    final BlockBody blockBodyDecodefromRLP =
        BlockBody.readFrom(bytesValueRLPInput, new MainnetBlockHeaderFunctions());

    assertThat(blockBodyDecodefromRLP)
        .isEqualTo(
            new BlockBody(
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.of(Collections.emptyList())));
  }

  @Test
  public void readBodyFieldsThrowsIfThereIsListWrappingBodyFields() {
    // [[txs],[ommers],[withdrawals]]
    // 0xc3c0c0c0
    final BytesValueRLPInput bytesValueRLPInput =
        new BytesValueRLPInput(Bytes.fromHexString("0xc3c0c0c0"), false);

    assertThrows(
        RLPException.class,
        () -> BlockBody.readFrom(bytesValueRLPInput, new MainnetBlockHeaderFunctions()));
  }

  private static class TransactionDeserializer extends StdDeserializer<Transaction> {
    protected TransactionDeserializer() {
      this(null);
    }

    protected TransactionDeserializer(final Class<?> vc) {
      super(vc);
    }

    @Override
    public Transaction deserialize(final JsonParser p, final DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      return new Transaction.Builder()
          .nonce(Bytes.fromHexStringLenient(node.get("nonce").asText()).toLong())
          .gasPrice(Wei.fromHexString(node.get("gasPrice").asText()))
          .gasLimit(Long.decode(node.get("gas").asText()))
          .to(Address.fromHexString(node.get("to").asText()))
          .value(Wei.fromHexString(node.get("value").asText()))
          .signature(
              new SECP256K1()
                  .createSignature(
                      new BigInteger(node.get("r").asText().substring(2), 16),
                      new BigInteger(node.get("s").asText().substring(2), 16),
                      recIdAndChainId(Byte.decode(node.get("v").asText())).getKey()))
          .payload(Bytes.fromHexString(node.get("input").asText()))
          .chainId(recIdAndChainId(Byte.decode(node.get("v").asText())).getValue())
          .build();
    }
  }

  private static Map.Entry<Byte, BigInteger> recIdAndChainId(final Byte vByte) {
    final BigInteger v = BigInteger.valueOf(vByte);
    final byte recId;
    BigInteger chainId = null;
    if (v.equals(REPLAY_UNPROTECTED_V_BASE) || v.equals(REPLAY_UNPROTECTED_V_BASE_PLUS_1)) {
      recId = v.subtract(REPLAY_UNPROTECTED_V_BASE).byteValueExact();
    } else if (v.compareTo(REPLAY_PROTECTED_V_MIN) > 0) {
      chainId = v.subtract(REPLAY_PROTECTED_V_BASE).divide(TWO);
      recId = v.subtract(TWO.multiply(chainId).add(REPLAY_PROTECTED_V_BASE)).byteValueExact();
    } else {
      throw new RuntimeException(
          String.format("An unsupported encoded `v` value of %s was found", v));
    }
    return Map.entry(recId, chainId);
  }

  public static class TestBlockBody extends BlockBody {
    @JsonCreator
    public TestBlockBody(
        @JsonProperty("Transactions") final List<Transaction> transactions,
        @JsonProperty("Uncles") final List<TestBlockHeader> uncles) {
      super(
          transactions.stream().collect(toUnmodifiableList()),
          uncles.stream().collect(toUnmodifiableList()));
    }
  }

  public static class TestBlockHeader extends BlockHeader {

    @JsonCreator
    public TestBlockHeader(
        @JsonProperty("parentHash") final String parentHash,
        @JsonProperty("sha3Uncles") final String uncleHash,
        @JsonProperty("miner") final String coinbase,
        @JsonProperty("stateRoot") final String stateRoot,
        @JsonProperty("transactionsRoot") final String transactionsTrie,
        @JsonProperty("receiptsRoot") final String receiptTrie,
        @JsonProperty("logsBloom") final String bloom,
        @JsonProperty("difficulty") final String difficulty,
        @JsonProperty("number") final String number,
        @JsonProperty("gasLimit") final String gasLimit,
        @JsonProperty("gasUsed") final String gasUsed,
        @JsonProperty("timestamp") final String timestamp,
        @JsonProperty("extraData") final String extraData,
        @JsonProperty("mixHash") final String mixHash,
        @JsonProperty("nonce") final String nonce,
        @JsonProperty("hash") final String __) {
      super(
          Hash.fromHexString(parentHash),
          Hash.fromHexString(uncleHash),
          Address.fromHexString(coinbase),
          Hash.fromHexString(stateRoot),
          Hash.fromHexString(transactionsTrie),
          Hash.fromHexString(receiptTrie),
          LogsBloomFilter.fromHexString(bloom),
          Difficulty.fromHexString(difficulty),
          Long.decode(number),
          Long.decode(gasLimit),
          Long.decode(gasUsed),
          Long.decode(timestamp),
          Bytes.fromHexString(extraData),
          null,
          Bytes32.wrap(Hash.fromHexString(mixHash).getBytes()),
          Bytes.fromHexStringLenient(nonce).toLong(),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          new MainnetBlockHeaderFunctions());
    }
  }

  private JsonNode parseTestFile(final String filename) throws IOException {
    return objectMapper.readTree(this.getClass().getResource("/" + filename));
  }
}
