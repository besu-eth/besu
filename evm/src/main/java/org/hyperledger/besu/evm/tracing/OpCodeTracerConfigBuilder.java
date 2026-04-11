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
package org.hyperledger.besu.evm.tracing;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Configuration for the default struct/opcode tracer. */
public final class OpCodeTracerConfigBuilder {
  private boolean traceStorage;
  private boolean traceMemory;
  private boolean traceStack;
  private boolean traceReturnData;
  private Set<String> traceOpcodes;
  private boolean eip3155Strict;
  private int limit;

  /**
   * Create an OpcodeTracerConfig builder from a previous built config.
   *
   * @param opCodeTracerConfig config to copy the fields from
   * @return a new builder with an immutable configuration
   */
  public static OpCodeTracerConfigBuilder createFrom(final OpCodeTracerConfig opCodeTracerConfig) {
    return new OpCodeTracerConfigBuilder(opCodeTracerConfig);
  }

  /**
   * Create an OpcodeTracerConfig builder from scratch
   *
   * @return a new builder
   */
  public static OpCodeTracerConfigBuilder create() {
    return new OpCodeTracerConfigBuilder();
  }

  private OpCodeTracerConfigBuilder() {}

  private OpCodeTracerConfigBuilder(final OpCodeTracerConfig opCodeTracerConfig) {
    Objects.requireNonNull(opCodeTracerConfig, "opCodeTracerConfig should not be null");
    this.traceStorage = opCodeTracerConfig.traceStorage();
    this.traceMemory = opCodeTracerConfig.traceMemory();
    this.traceStack = opCodeTracerConfig.traceStack();
    this.traceReturnData = opCodeTracerConfig.traceReturnData();
    this.traceOpcodes = opCodeTracerConfig.traceOpcodes();
    this.eip3155Strict = opCodeTracerConfig.eip3155Strict();
    this.limit = opCodeTracerConfig.limit();
  }

  /**
   * Set storage tracing flag.
   *
   * @param enable flag to enable tracing of storage
   * @return the current builder
   */
  public OpCodeTracerConfigBuilder traceStorage(final boolean enable) {
    traceStorage = enable;
    return this;
  }

  /**
   * Set memory tracing flag.
   *
   * @param enable flag to enable tracing of memory
   * @return the current builder
   */
  public OpCodeTracerConfigBuilder traceMemory(final boolean enable) {
    traceMemory = enable;
    return this;
  }

  /**
   * Set stack tracing flag.
   *
   * @param enable flag to enable tracing of stack
   * @return the current builder
   */
  public OpCodeTracerConfigBuilder traceStack(final boolean enable) {
    traceStack = enable;
    return this;
  }

  /**
   * Set returnData tracing flag.
   *
   * @param enable flag to enable tracing of returnData
   * @return the current builder
   */
  public OpCodeTracerConfigBuilder traceReturnData(final boolean enable) {
    traceReturnData = enable;
    return this;
  }

  /**
   * Sets the list of opcodes to trace
   *
   * @param traceOpcodes list of opcodes to trace
   * @return the current builder
   */
  public OpCodeTracerConfigBuilder traceOpcodes(final Set<String> traceOpcodes) {
    this.traceOpcodes = traceOpcodes.stream().map(String::toLowerCase).collect(Collectors.toSet());
    return this;
  }
  /**
   * Set the maximum number of steps to trace. Zero means unlimited.
   *
   * @param n maximum number of steps, or 0 for unlimited
   * @return the current builder
   */
  public OpCodeTracerConfigBuilder limit(final int n) {
    if (n < 0) throw new IllegalArgumentException("limit must be >= 0, got: " + n);
    limit = n;
    return this;
  }

  /**
   * Set eip3155Strict flag.
   *
   * @param enable flag to enable eip3155 mode tracing
   * @return the current builder
   */
  public OpCodeTracerConfigBuilder eip3155Strict(final boolean enable) {
    eip3155Strict = enable;
    return this;
  }

  /**
   * Build OpCodeTracerConfig configuration.
   *
   * @return the config
   */
  public OpCodeTracerConfig build() {
    return new Config(
        traceStorage, traceMemory, traceStack, traceReturnData, traceOpcodes, eip3155Strict, limit);
  }

  /**
   * Interface for the OpCodeTracerConfig. This interface is backed by a single record to avoid
   * rewriting all the boilerplate code for constructors, equals, hashcode, fields, etc... Also this
   * way no external entity can create an OpCodeTracerConfig than not through the builder provided
   * in this class.
   */
  public sealed interface OpCodeTracerConfig permits Config {
    /** static default OpcodeTracerConfig which can be accessed externally */
    OpCodeTracerConfig DEFAULT =
        new Config(true, false, true, false, Collections.emptySet(), false, 0);

    /**
     * Check if tracing of storage is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean traceStorage();

    /**
     * Check if tracing of memory is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean traceMemory();

    /**
     * Check if tracing of stack is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean traceStack();

    /**
     * Check if tracing of returnData is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean traceReturnData();

    /**
     * List of opcodes that the tracer should exclusively trace for. If empty, all opcodes are
     * traced.
     *
     * @return list of opcodes to trace.
     */
    Set<String> traceOpcodes();

    /**
     * Check if tracing in eip3155 mode is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean eip3155Strict();

    /**
     * Maximum number of steps to trace. Zero means unlimited.
     *
     * @return the step limit, or 0 for unlimited
     */
    int limit();
  }

  private record Config(
      boolean traceStorage,
      boolean traceMemory,
      boolean traceStack,
      boolean traceReturnData,
      Set<String> traceOpcodes,
      boolean eip3155Strict,
      int limit)
      implements OpCodeTracerConfig {}
}
