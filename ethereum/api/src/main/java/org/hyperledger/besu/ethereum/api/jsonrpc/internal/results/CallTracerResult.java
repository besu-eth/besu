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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.MoreObjects;
import org.apache.tuweni.bytes.Bytes;

/**
 * Represents the result format for Ethereum's callTracer as specified in the Geth documentation.
 *
 * <p>The callTracer is a built-in tracer that captures information about all calls made during
 * transaction execution, including regular calls, delegate calls, static calls, and contract
 * creation calls. This class provides a structured representation of the trace data that matches
 * the JSON format expected by Ethereum clients.
 *
 * <p>The JSON output follows the format specified at: <a
 * href="https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers#call-tracer">Geth
 * CallTracer Documentation</a>
 *
 * <p>Example JSON output:
 *
 * <pre>{@code
 * {
 *   "type": "CALL",
 *   "from": "0x1234567890123456789012345678901234567890",
 *   "to": "0x0987654321098765432109876543210987654321",
 *   "value": "0x0",
 *   "gas": "0x5208",
 *   "gasUsed": "0x5208",
 *   "input": "0x",
 *   "output": "0x",
 *   "calls": [...]
 * }
 * }</pre>
 *
 * @see <a
 *     href="https://geth.ethereum.org/docs/developers/evm-tracing/built-in-tracers#call-tracer">
 *     Geth CallTracer Documentation</a>
 */
@JsonPropertyOrder({
  "from",
  "gas",
  "gasUsed",
  "to",
  "input",
  "output",
  "error",
  "revertReason",
  "calls",
  "logs",
  "value",
  "type",
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CallTracerResult {

  /** The type of call (e.g., CALL, DELEGATECALL, STATICCALL, CREATE, CREATE2) */
  private String type;

  /** The address that initiated the call */
  private String from;

  /** The address that received the call */
  private String to;

  /** The value transferred in the call (in wei, as hex string) */
  private String value;

  /** The amount of gas allocated for the call */
  private BigInteger gas;

  /** The amount of gas actually used by the call */
  private BigInteger gasUsed;

  /** The input data sent with the call (as hex string) */
  private String input;

  /** The output data returned by the call (as hex string) */
  private String output;

  /** Error message if the call failed */
  private String error;

  /** Revert reason if the call was reverted */
  private String revertReason;

  /** List of nested calls made during this call's execution */
  private List<CallTracerResult> calls;

  /** Logs emitted by this call itself, only collected when the withLog option is set */
  private List<CallLog> logs;

  /**
   * Private constructor used by the Builder pattern.
   *
   * @param builder the builder instance containing the field values
   */
  private CallTracerResult(final Builder builder) {
    this.type = builder.type;
    this.from = builder.from;
    this.to = builder.to;
    this.value = builder.value;
    this.gas = builder.gas;
    this.gasUsed = builder.gasUsed;
    this.input = builder.input;
    this.output = builder.output;
    this.error = builder.error;
    this.revertReason = builder.revertReason;
    this.calls = builder.calls;
    this.logs = builder.logs;
  }

  /** Default constructor required for Jackson JSON deserialization. */
  public CallTracerResult() {}

  /**
   * Gets the type of call operation.
   *
   * @return the call type (e.g., "CALL", "DELEGATECALL", "STATICCALL", "CREATE", "CREATE2",
   *     "SELFDESTRUCT")
   */
  @JsonGetter("type")
  public String getType() {
    return type;
  }

  /**
   * Gets the address that initiated the call.
   *
   * @return the sender address as a hex string, or null if not applicable
   */
  @JsonGetter("from")
  public String getFrom() {
    return from;
  }

  /**
   * Gets the address that received the call.
   *
   * @return the recipient address as a hex string, or null if not applicable (e.g., for CREATE)
   */
  @JsonGetter("to")
  public String getTo() {
    return to;
  }

  /**
   * Gets the value transferred in the call.
   *
   * @return the value in wei as a hex string (e.g., "0x0"), or null if no value was transferred
   */
  @JsonGetter("value")
  public String getValue() {
    return value;
  }

  /**
   * Gets the amount of gas allocated for the call.
   *
   * @return the gas limit as a hex string (e.g., "0x5208"), or null if not set
   */
  @JsonGetter("gas")
  public String getGas() {
    return toHexString(gas);
  }

  /**
   * Gets the amount of gas actually consumed by the call.
   *
   * @return the gas used as a hex string (e.g., "0x5208"), or null if not available
   */
  @JsonGetter("gasUsed")
  public String getGasUsed() {
    return toHexString(gasUsed);
  }

  /**
   * Gets the input data sent with the call.
   *
   * @return the input data as a hex string (e.g., "0x1234abcd"), or null if no input data
   */
  @JsonGetter("input")
  public String getInput() {
    return input;
  }

  /**
   * Gets the output data returned by the call.
   *
   * @return the output data as a hex string (e.g., "0x1234abcd"), or null if no output data
   */
  @JsonGetter("output")
  public String getOutput() {
    return output;
  }

  /**
   * Gets the error message if the call failed.
   *
   * @return the error message, or null if the call was successful
   */
  @JsonGetter("error")
  public String getError() {
    return error;
  }

  /**
   * Gets the revert reason if the call was reverted.
   *
   * @return the revert reason, or null if the call was not reverted
   */
  @JsonGetter("revertReason")
  public String getRevertReason() {
    return revertReason;
  }

  /**
   * Gets the list of nested calls made during this call's execution.
   *
   * @return the list of nested calls, or null if no nested calls were made
   */
  @JsonGetter("calls")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<CallTracerResult> getCalls() {
    return calls;
  }

  /**
   * Gets the logs emitted by this call itself.
   *
   * @return the logs, or null if withLog was not set or no log was emitted
   */
  @JsonGetter("logs")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<CallLog> getLogs() {
    return logs;
  }

  /**
   * Converts a BigInteger to a hex string with "0x" prefix.
   *
   * @param value the BigInteger value to convert
   * @return the hex string representation, or null if the input is null
   */
  private String toHexString(final BigInteger value) {
    if (value == null) {
      return null;
    }
    return "0x" + value.toString(16);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .add("from", from)
        .add("to", to)
        .add("value", value)
        .add("gas", gas)
        .add("gasUsed", gasUsed)
        .add("input", input)
        .add("output", output)
        .add("error", error)
        .add("revertReason", revertReason)
        .add("calls", calls)
        .toString();
  }

  /**
   * Creates a new Builder instance for constructing CallTracerResult objects.
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing CallTracerResult instances using the Builder pattern.
   *
   * <p>This builder provides a fluent interface for setting the various fields of a
   * CallTracerResult. It supports method chaining and provides convenience methods for common
   * operations like adding nested calls.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * CallTracerResult result = CallTracerResult.builder()
   *     .type("CALL")
   *     .from("0x1234...")
   *     .to("0x5678...")
   *     .gas(21000L)
   *     .gasUsed(21000L)
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private String type;
    private String from;
    private String to;
    private String value;
    private BigInteger gas;
    private BigInteger gasUsed;
    private String input;
    private String output;
    private String error;
    private String revertReason;
    private List<CallTracerResult> calls;
    private List<CallLog> logs;

    /** Private constructor to enforce use of the static factory method. */
    private Builder() {}

    /**
     * Sets the type of call operation.
     *
     * @param type the call type (e.g., "CALL", "DELEGATECALL", "STATICCALL", "CREATE", "CREATE2")
     * @return this builder instance for method chaining
     */
    public Builder type(final String type) {
      this.type = type;
      return this;
    }

    /**
     * Sets the address that initiated the call.
     *
     * @param from the sender address as a hex string
     * @return this builder instance for method chaining
     */
    public Builder from(final String from) {
      this.from = from;
      return this;
    }

    /**
     * Sets the address that received the call.
     *
     * @param to the recipient address as a hex string
     * @return this builder instance for method chaining
     */
    public Builder to(final String to) {
      this.to = to;
      return this;
    }

    /**
     * Sets the value transferred in the call.
     *
     * @param value the value in wei as a hex string
     * @return this builder instance for method chaining
     */
    public Builder value(final String value) {
      this.value = value;
      return this;
    }

    /**
     * Sets the amount of gas allocated for the call.
     *
     * @param gas the gas limit as a long value
     * @return this builder instance for method chaining
     */
    public Builder gas(final long gas) {
      this.gas = BigInteger.valueOf(gas);
      return this;
    }

    /**
     * Sets the amount of gas actually consumed by the call.
     *
     * @param gasUsed the gas used as a long value
     * @return this builder instance for method chaining
     */
    public Builder gasUsed(final long gasUsed) {
      this.gasUsed = BigInteger.valueOf(gasUsed);
      return this;
    }

    /**
     * Sets the input data sent with the call.
     *
     * @param input the input data as a hex string
     * @return this builder instance for method chaining
     */
    public Builder input(final String input) {
      this.input = input;
      return this;
    }

    /**
     * Sets the output data returned by the call.
     *
     * @param output the output data as a hex string
     * @return this builder instance for method chaining
     */
    public Builder output(final String output) {
      this.output = output;
      return this;
    }

    /**
     * Sets the error message if the call failed.
     *
     * @param error the error message
     * @return this builder instance for method chaining
     */
    public Builder error(final String error) {
      this.error = error;
      return this;
    }

    /**
     * Sets the revert reason if the call was reverted.
     *
     * @param revertReason the revert reason as raw bytes (will be decoded)
     * @return this builder instance for method chaining
     */
    public Builder revertReason(final Bytes revertReason) {
      this.revertReason =
          JsonRpcErrorResponse.decodeRevertReason(revertReason).orElse(revertReason.toHexString());
      return this;
    }

    /**
     * Sets the revert reason directly from a decoded string.
     *
     * <p>This method should be used when you have already decoded the revert reason and want to set
     * it directly without any fallback to hex encoding. This is useful for Geth compatibility where
     * revertReason should only be set if it's a valid decoded Error(string).
     *
     * @param revertReason the decoded revert reason string
     * @return this builder instance for method chaining
     */
    public Builder revertReasonDecoded(final String revertReason) {
      this.revertReason = revertReason;
      return this;
    }

    /**
     * Sets the list of nested calls made during this call's execution.
     *
     * @param calls the list of nested CallTracerResult objects
     * @return this builder instance for method chaining
     */
    public Builder calls(final List<CallTracerResult> calls) {
      this.calls = calls;
      return this;
    }

    /**
     * Adds a single nested call to the list of calls.
     *
     * <p>If the calls list is null, it will be initialized as an empty ArrayList.
     *
     * @param call the nested CallTracerResult to add
     * @return this builder instance for method chaining
     */
    public Builder addCall(final CallTracerResult call) {
      if (this.calls == null) {
        this.calls = new ArrayList<>();
      }
      this.calls.add(call);
      return this;
    }

    /**
     * Adds a log emitted directly by this call.
     *
     * @param log the log to add
     * @return this builder instance for method chaining
     */
    public Builder addLog(final CallLog log) {
      if (this.logs == null) {
        this.logs = new ArrayList<>();
      }
      this.logs.add(log);
      return this;
    }

    /**
     * Number of nested calls added so far
     *
     * @return the number of nested calls
     */
    public int callCount() {
      return this.calls == null ? 0 : this.calls.size();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("type", type)
          .add("from", from)
          .add("to", to)
          .add("value", value)
          .add("gas", gas)
          .add("gasUsed", gasUsed)
          .add("input", input)
          .add("output", output)
          .add("error", error)
          .add("revertReason", revertReason)
          .add("calls", calls)
          .toString();
    }

    /**
     * Builds and returns a new CallTracerResult instance with the configured values.
     *
     * @return a new CallTracerResult instance
     */
    public CallTracerResult build() {
      return new CallTracerResult(this);
    }

    /**
     * Return Gas value
     *
     * @return Gas value
     */
    public BigInteger getGas() {
      return this.gas;
    }

    /**
     * Return call type
     *
     * @return call type
     */
    public String getType() {
      return this.type;
    }

    /**
     * Gas Used
     *
     * @return Gas Used
     */
    public BigInteger getGasUsed() {
      return this.gasUsed;
    }

    /**
     * To address
     *
     * @return the To address
     */
    public String getTo() {
      return this.to;
    }

    public String getFrom() {
      return this.from;
    }

    public String getValue() {
      return this.value;
    }
  }

  /** One log of a call frame, as reported by the callTracer withLog option. */
  @JsonPropertyOrder({"address", "topics", "data", "position", "index"})
  public static class CallLog {
    private final String address;
    private final List<String> topics;
    private final String data;
    private final int position;
    private Integer index;

    /**
     * Creates a call log from an EVM log.
     *
     * @param log the emitted log
     * @param position the number of nested calls the frame had made when the log was emitted
     */
    public CallLog(final Log log, final int position) {
      this.address = log.getLogger().toString();
      this.topics = log.getTopics().stream().map(LogTopic::toString).toList();
      this.data = log.getData().toString();
      this.position = position;
    }

    /**
     * Gets the address of the contract that emitted the log.
     *
     * @return the address as a hex string
     */
    @JsonGetter("address")
    public String getAddress() {
      return address;
    }

    /**
     * Gets the log topics.
     *
     * @return the topics as hex strings
     */
    @JsonGetter("topics")
    public List<String> getTopics() {
      return topics;
    }

    /**
     * Gets the log data.
     *
     * @return the data as a hex string
     */
    @JsonGetter("data")
    public String getData() {
      return data;
    }

    /**
     * Gets the number of nested calls the frame had made when the log was emitted.
     *
     * @return the position as a hex quantity
     */
    @JsonGetter("position")
    public String getPosition() {
      return Quantity.create(position);
    }

    /**
     * Gets the index of the log within the block, equal to the receipt logIndex.
     *
     * @return the index as a hex quantity, or null if the log did not take effect
     */
    @JsonGetter("index")
    public String getIndex() {
      return index == null ? null : Quantity.create(index);
    }

    /**
     * Sets the index of the log within the block.
     *
     * @param index the block-wide log index
     */
    public void setIndex(final int index) {
      this.index = index;
    }
  }
}
