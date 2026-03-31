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
package org.hyperledger.besu.evm.internal;

import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * The type Evm configuration.
 *
 * @param jumpDestCacheWeightKB the jump destination cache weight in kb
 * @param worldUpdaterMode the world updater mode
 * @param enableOptimizedOpcodes enable optimized implementation of certain opcodes in the EVM
 * @param enableEvmV2 enable experimental EVM v2 with long[] stack representation
 * @param evmStackSize the maximum evm stack size
 * @param maxCodeSizeOverride An optional override of the maximum code size set by the EVM fork
 * @param maxInitcodeSizeOverride An optional override of the maximum initcode size set by the EVM
 *     fork
 */
public record EvmConfiguration(
    long jumpDestCacheWeightKB,
    WorldUpdaterMode worldUpdaterMode,
    boolean enableOptimizedOpcodes,
    boolean enableEvmV2,
    Integer evmStackSize,
    Optional<Integer> maxCodeSizeOverride,
    Optional<Integer> maxInitcodeSizeOverride) {

  /** How should the world state update be handled within transactions? */
  public enum WorldUpdaterMode {
    /**
     * Stack updates, requiring original account and storage values to read through the whole stack
     */
    STACKED,
    /** Share a single state for accounts and storage values, undoing changes on reverts. */
    JOURNALED
  }

  /** The constant DEFAULT. */
  public static final EvmConfiguration DEFAULT =
      new EvmConfiguration(32_000L, WorldUpdaterMode.STACKED, true, false);

  /**
   * Create an EVM Configuration without any overrides
   *
   * @param jumpDestCacheWeightKilobytes the jump dest cache weight (in kibibytes)
   * @param worldstateUpdateMode the world update mode
   * @param enableOptimizedOpcodes enabled opcode optimizations
   */
  public EvmConfiguration(
      final Long jumpDestCacheWeightKilobytes,
      final WorldUpdaterMode worldstateUpdateMode,
      final boolean enableOptimizedOpcodes) {
    this(
        jumpDestCacheWeightKilobytes,
        worldstateUpdateMode,
        enableOptimizedOpcodes,
        false,
        MessageFrame.DEFAULT_MAX_STACK_SIZE,
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Create an EVM Configuration without any overrides, with explicit EVM v2 flag
   *
   * @param jumpDestCacheWeightKilobytes the jump dest cache weight (in kibibytes)
   * @param worldstateUpdateMode the world update mode
   * @param enableOptimizedOpcodes enabled opcode optimizations
   * @param enableEvmV2 enable experimental EVM v2 with long[] stack representation
   */
  public EvmConfiguration(
      final Long jumpDestCacheWeightKilobytes,
      final WorldUpdaterMode worldstateUpdateMode,
      final boolean enableOptimizedOpcodes,
      final boolean enableEvmV2) {
    this(
        jumpDestCacheWeightKilobytes,
        worldstateUpdateMode,
        enableOptimizedOpcodes,
        enableEvmV2,
        MessageFrame.DEFAULT_MAX_STACK_SIZE,
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Gets jump dest cache weight bytes.
   *
   * @return the jump dest cache weight bytes
   */
  public long getJumpDestCacheWeightBytes() {
    return jumpDestCacheWeightKB * 1024L;
  }

  /**
   * Update the configuration with new overrides, or clearing the overrides with {@link
   * Optional#empty}
   *
   * @param newMaxCodeSize a new max code size override
   * @param newMaxInitcodeSize a new max initcode size override
   * @param newEvmStackSize a new EVM stack size override
   * @return the updated EVM configuration
   */
  public EvmConfiguration overrides(
      final OptionalInt newMaxCodeSize,
      final OptionalInt newMaxInitcodeSize,
      final OptionalInt newEvmStackSize) {
    return new EvmConfiguration(
        jumpDestCacheWeightKB,
        worldUpdaterMode,
        enableOptimizedOpcodes,
        enableEvmV2,
        newEvmStackSize.orElse(MessageFrame.DEFAULT_MAX_STACK_SIZE),
        newMaxCodeSize.isPresent() ? Optional.of(newMaxCodeSize.getAsInt()) : Optional.empty(),
        newMaxInitcodeSize.isPresent()
            ? Optional.of(newMaxInitcodeSize.getAsInt())
            : Optional.empty());
  }
}
