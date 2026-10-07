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

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Bloom filter implementation for storing persistent logs, describes a 2048-bit representation of
 * all log entries of a transaction, except data. Sets the bits of the 2048 byte array, where
 * indices are given by: The lower order 11-bits, of the first three double-bytes, of the KECCAK256,
 * of each value. For instance the address "0x0F572E5295C57F15886F9B263E2F6D2D6C7B5EC6" results in
 * the KECCAK256 hash "bd2b01afcd27800b54d2179edc49e2bffde5078bb6d0b204694169b1643fb108", of which
 * the corresponding double-bytes are: bd2b, 01af, cd27, corresponding to the following bits in the
 * bloom filter: 1323, 431, 1319
 */
public class LogsBloomFilter extends BytesHolder {

  /** The constant BYTE_SIZE. */
  public static final int BYTE_SIZE = 256;

  private static final int LEAST_SIGNIFICANT_BYTE = 0xFF;
  private static final int LEAST_SIGNIFICANT_THREE_BITS = 0x7;
  private static final int BITS_IN_BYTE = 8;

  /** Instantiates a new Logs bloom filter. */
  public LogsBloomFilter() {
    super(Bytes.wrap(new byte[BYTE_SIZE]));
  }

  /**
   * Instantiates a new Logs bloom filter.
   *
   * @param data the data
   */
  public LogsBloomFilter(final MutableBytes data) {
    this(data.copy());
  }

  /**
   * Instantiates a new Logs bloom filter.
   *
   * @param data the data
   */
  @JsonCreator
  public LogsBloomFilter(final Bytes data) {
    super(data);
    checkArgument(
        data.size() == BYTE_SIZE,
        "Invalid size for bloom filter backing array: expected %s but got %s",
        BYTE_SIZE,
        data.size());
  }

  /**
   * Instantiates a new Logs bloom filter.
   *
   * @param logsBloomHexString the logs bloom hex string
   */
  public LogsBloomFilter(final String logsBloomHexString) {
    this(Bytes.fromHexString(logsBloomHexString));
  }

  /**
   * Instantiate logs bloom filter From hex string.
   *
   * @param hexString the hex string
   * @return the logs bloom filter
   */
  public static LogsBloomFilter fromHexString(final String hexString) {
    return new LogsBloomFilter(Bytes.fromHexString(hexString));
  }

  /**
   * Instantiates an empty logs bloom filter.
   *
   * @return the logs bloom filter
   */
  public static LogsBloomFilter empty() {
    return new LogsBloomFilter(Bytes.wrap(new byte[LogsBloomFilter.BYTE_SIZE]));
  }

  /**
   * Creates a bloom filter from the given RLP-encoded input.
   *
   * @param input The input to read from
   * @return the input's corresponding bloom filter
   */
  public static LogsBloomFilter readFrom(final RLPInput input) {
    final Bytes bytes = input.readBytes();
    if (bytes.size() != BYTE_SIZE) {
      throw new RLPException(
          String.format(
              "LogsBloomFilter unexpected size of %s (needs %s)", bytes.size(), BYTE_SIZE));
    }
    return new LogsBloomFilter(bytes);
  }

  /**
   * Checks of subset log bloom filter can be contained.
   *
   * @param subset the subset
   * @return the boolean
   */
  public boolean couldContain(final LogsBloomFilter subset) {
    if (subset == null) {
      return true;
    }
    if (getBytes().size() != subset.getBytes().size()) {
      return false;
    }
    for (int i = 0; i < getBytes().size(); i++) {
      final byte subsetValue = subset.getBytes().get(i);
      if ((getBytes().get(i) & subsetValue) != subsetValue) {
        return false;
      }
    }
    return true;
  }

  /**
   * Instantiate Builder.
   *
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** The Builder. */
  public static final class Builder {
    /** The Data. */
    final MutableBytes data;

    private Builder() {
      data = MutableBytes.create(LogsBloomFilter.BYTE_SIZE);
    }

    /**
     * Insert filter.
     *
     * @param other the other
     * @return the builder
     */
    public Builder insertFilter(final LogsBloomFilter other) {
      for (int i = 0; i < data.size(); ++i) {
        data.set(i, (byte) ((data.get(i) | other.getBytes().get(i)) & 0xff));
      }
      return this;
    }

    /**
     * Insert log.
     *
     * @param log the log
     * @return the builder
     */
    public Builder insertLog(final Log log) {
      insertBytes(log.getLogger().getBytes());

      for (final LogTopic topic : log.getTopics()) {
        insertBytes(topic.getBytes());
      }
      return this;
    }

    /**
     * Insert raw log.
     *
     * @param loggerAddress the address of the logger
     * @param logTopics the log topics
     * @return the builder
     */
    public Builder insertRawLog(final Bytes loggerAddress, final List<Bytes> logTopics) {
      insertBytes(loggerAddress);
      for (Bytes logTopic : logTopics) {
        insertBytes(logTopic);
      }
      return this;
    }

    /**
     * Insert logs.
     *
     * @param logs the logs
     * @return the builder
     */
    public Builder insertLogs(final Collection<Log> logs) {
      logs.forEach(this::insertLog);
      return this;
    }

    /**
     * Insert raw logs.
     *
     * @param logs the logs with each log separated into components, ordered like [logger
     *     address][topics...][data]
     * @return the builder
     */
    public Builder insertRawLogs(final Collection<List<Bytes>> logs) {
      logs.forEach(
          (bytesList) ->
              insertRawLog(bytesList.getFirst(), bytesList.subList(1, bytesList.size() - 1)));
      return this;
    }

    /**
     * Insert bytes.
     *
     * @param value the value
     * @return the builder
     */
    public Builder insertBytes(final Bytes value) {
      setBits(keccak256(value));
      return this;
    }

    /**
     * Build logs bloom filter.
     *
     * @return the logs bloom filter
     */
    public LogsBloomFilter build() {
      return new LogsBloomFilter(data);
    }

    /**
     * Discover the low order 11-bits, of the first three double-bytes, of the KECCAK256 hash, of
     * each value and update the bloom filter accordingly.
     *
     * @param hashValue The hash of the log item.
     */
    private void setBits(final Bytes hashValue) {
      for (int counter = 0; counter < 6; counter += 2) {
        final int setBloomBit =
            ((hashValue.get(counter) & LEAST_SIGNIFICANT_THREE_BITS) << BITS_IN_BYTE)
                + (hashValue.get(counter + 1) & LEAST_SIGNIFICANT_BYTE);
        setBit(setBloomBit);
      }
    }

    private void setBit(final int index) {
      final int byteIndex = BYTE_SIZE - 1 - index / 8;
      final int bitIndex = index % 8;
      data.set(byteIndex, (byte) (data.get(byteIndex) | (1 << bitIndex)));
    }
  }
}
