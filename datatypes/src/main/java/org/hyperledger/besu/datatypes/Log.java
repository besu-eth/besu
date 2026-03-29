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
package org.hyperledger.besu.datatypes;

import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * A log entry is a tuple of a logger’s address (the address of the contract that added the logs), a
 * series of 32-bytes log topics, and some number of bytes of data.
 */
public class Log {

  private final Address logger;
  private final Bytes data;
  private final ImmutableList<LogTopic> topics;

  /**
   * Instantiates a new Log.
   *
   * @param logger The address of the contract that produced this log.
   * @param data Data associated with this log.
   * @param topics Indexable topics associated with this log.
   */
  @JsonCreator
  public Log(
      @JsonProperty("logger") final Address logger,
      @JsonProperty("data") final Bytes data,
      @JsonProperty("topics") final List<LogTopic> topics) {
    this.logger = logger;
    this.data = data;
    this.topics = ImmutableList.copyOf(topics);
  }

  /**
   * Writes the log entry to the provided RLP output.
   *
   * @param out the output in which to encode the log entry.
   */
  public void writeTo(final RLPOutput out) {
    writeTo(out, false);
  }

  /**
   * Writes the log entry to the provided RLP output.
   *
   * @param out the output in which to encode the log entry.
   * @param compacted whether to compact the rlp log entry by trimming leading zeros on topics and
   *     data.
   */
  public void writeTo(final RLPOutput out, final boolean compacted) {
    out.startList();
    out.writeBytes(logger.getBytes());
    if (compacted) {
      out.writeList(topics, (topic, listOut) -> encodeTrimmedData(listOut, topic.getBytes()));
      encodeTrimmedData(out, data);
    } else {
      out.writeList(topics, (topic, listOut) -> listOut.writeBytes(topic.getBytes()));
      out.writeBytes(data);
    }
    out.endList();
  }

  private void encodeTrimmedData(final RLPOutput rlpOutput, final Bytes data) {
    rlpOutput.startList();
    final Bytes shortData = data.trimLeadingZeros();
    final int zeroLeadDataSize = data.size() - shortData.size();
    rlpOutput.writeIntScalar(zeroLeadDataSize);
    rlpOutput.writeBytes(shortData);
    rlpOutput.endList();
  }

  /**
   * Reads the log entry from the provided RLP input.
   *
   * @param in the input from which to decode the log entry.
   * @return the read log entry.
   */
  public static Log readFrom(final RLPInput in) {
    return readFrom(in, false);
  }

  /**
   * Reads the log entry from the provided RLP input.
   *
   * @param in the input from which to decode the log entry.
   * @param compacted whether to compact the rlp log entry by trimming leading zeros on topics and
   *     data.
   * @return the read log entry.
   */
  public static Log readFrom(final RLPInput in, final boolean compacted) {
    in.enterList();
    final Address logger = Address.wrap(in.readBytes());

    final List<LogTopic> topics;
    final Bytes data;
    if (compacted) {
      // Topics and data may be in canonical compacted format ([leadingZeros, shortData] lists)
      // or raw bytes32/bytes (wire format). Autodetect per-item using nextIsList().
      topics =
          in.readList(
              listIn ->
                  LogTopic.wrap(
                      listIn.nextIsList()
                          ? Bytes32.wrap(readTrimmedData(listIn))
                          : listIn.readBytes32()));
      data = in.nextIsList() ? readTrimmedData(in) : in.readBytes();
    } else {
      topics = in.readList(listIn -> LogTopic.wrap(listIn.readBytes32()));
      data = in.readBytes();
    }

    in.leaveList();
    return new Log(logger, data, topics);
  }

  private static Bytes readTrimmedData(final RLPInput in) {
    in.enterList();
    final int zeroLeadDataSize = in.readIntScalar();
    final Bytes shortData = in.readBytes();
    final MutableBytes data = MutableBytes.create(zeroLeadDataSize + shortData.size());
    data.set(zeroLeadDataSize, shortData);
    in.leaveList();
    return data;
  }

  /**
   * Gets logger address.
   *
   * @return the logger
   */
  @JsonProperty("logger")
  public Address getLogger() {
    return logger;
  }

  /**
   * Gets data.
   *
   * @return the data
   */
  @JsonProperty("data")
  public Bytes getData() {
    return data;
  }

  /**
   * Gets topics.
   *
   * @return the topics
   */
  @JsonProperty("topics")
  public List<LogTopic> getTopics() {
    return topics;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof Log that)) return false;

    // Compare data
    return this.data.equals(that.data)
        && this.logger.equals(that.logger)
        && this.topics.equals(that.topics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, logger, topics);
  }

  @Override
  public String toString() {
    final String joinedTopics = Joiner.on("\n").join(topics);
    return String.format("Data: %s\nLogger: %s\nTopics: %s", data, logger, joinedTopics);
  }
}
