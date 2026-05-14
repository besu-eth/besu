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
package org.hyperledger.besu.evm.precompile;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.besu.evm.precompile.AbstractPrecompiledContract.cacheEventConsumer;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.nativelib.gnark.LibGnarkEIP2537;

import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.sun.jna.ptr.IntByReference;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Abstract BLS12 precompiled contract. */
public abstract class AbstractBLS12PrecompiledContract implements PrecompiledContract {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBLS12PrecompiledContract.class);

  static {
    // set parallel 1 for testing.  Remove this for prod code (or set a rational limit)
    // LibGnarkEIP2537.setDegreeOfMSMParallelism(1);
  }

  /** Default result caching to false unless otherwise set. */
  protected static Boolean enableResultCaching = Boolean.FALSE;

  /** The Discount table. */
  static final int[] G1_DISCOUNT_TABLE =
      new int[] {
        -1, 1000, 949, 848, 797, 764, 750, 738, 728, 719, 712, 705, 698, 692, 687, 682, 677, 673,
        669, 665, 661, 658, 654, 651, 648, 645, 642, 640, 637, 635, 632, 630, 627, 625, 623, 621,
        619, 617, 615, 613, 611, 609, 608, 606, 604, 603, 601, 599, 598, 596, 595, 593, 592, 591,
        589, 588, 586, 585, 584, 582, 581, 580, 579, 577, 576, 575, 574, 573, 572, 570, 569, 568,
        567, 566, 565, 564, 563, 562, 561, 560, 559, 558, 557, 556, 555, 554, 553, 552, 551, 550,
        549, 548, 547, 547, 546, 545, 544, 543, 542, 541, 540, 540, 539, 538, 537, 536, 536, 535,
        534, 533, 532, 532, 531, 530, 529, 528, 528, 527, 526, 525, 525, 524, 523, 522, 522, 521,
        520, 520, 519
      };

  static final int[] G2_DISCOUNT_TABLE =
      new int[] {
        -1, 1000, 1000, 923, 884, 855, 832, 812, 796, 782, 770, 759, 749, 740, 732, 724, 717, 711,
        704, 699, 693, 688, 683, 679, 674, 670, 666, 663, 659, 655, 652, 649, 646, 643, 640, 637,
        634, 632, 629, 627, 624, 622, 620, 618, 615, 613, 611, 609, 607, 606, 604, 602, 600, 598,
        597, 595, 593, 592, 590, 589, 587, 586, 584, 583, 582, 580, 579, 578, 576, 575, 574, 573,
        571, 570, 569, 568, 567, 566, 565, 563, 562, 561, 560, 559, 558, 557, 556, 555, 554, 553,
        552, 552, 551, 550, 549, 548, 547, 546, 545, 545, 544, 543, 542, 541, 541, 540, 539, 538,
        537, 537, 536, 535, 535, 534, 533, 532, 532, 531, 530, 530, 529, 528, 528, 527, 526, 526,
        525, 524, 524
      };

  /** The Max discount. */
  static final int G1_MAX_DISCOUNT = 519;

  static final int G2_MAX_DISCOUNT = 524;

  private final String name;
  private final byte operationId;
  private final int inputLimit;

  /**
   * Instantiates a new Abstract BLS12 precompiled contract.
   *
   * @param name the name
   * @param operationId the operation id
   * @param inputLen the input len
   */
  AbstractBLS12PrecompiledContract(final String name, final byte operationId, final int inputLen) {
    this.name = name;
    this.operationId = operationId;
    this.inputLimit = inputLen + 1;
  }

  /**
   * Is bls12 supported on this platform
   *
   * @return true if the native library was loaded.
   */
  public static boolean isAvailable() {
    try {
      return LibGnarkEIP2537.ENABLED;
    } catch (UnsatisfiedLinkError | NoClassDefFoundError ule) {
      LOG.info("bls12-381 native precompile not available: {}", ule.getMessage());
    }
    return false;
  }

  @Override
  public String getName() {
    return name;
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {

    PrecompileInputResultTuple res = null;

    Integer cacheKey = null;
    final Bytes cachedInput = input.size() > inputLimit ? input.slice(0, inputLimit) : input;

    if (enableResultCaching) {
      cacheKey = AbstractPrecompiledContract.getCacheKey(input, inputLimit);
      res = getCache().getIfPresent(cacheKey);
      if (res != null) {
        if (res.cachedInput().equals(cachedInput)) {
          cacheEventConsumer.accept(
              new AbstractPrecompiledContract.CacheEvent(
                  name, AbstractPrecompiledContract.CacheMetric.HIT));
          return res.cachedResult();
        } else {
          LOG.debug(
              "false positive {} {}, cache key {}, cached input: {}, input: {}",
              name,
              input.getClass().getSimpleName(),
              cacheKey,
              res.cachedInput().toHexString(),
              cachedInput.toHexString());

          cacheEventConsumer.accept(
              new AbstractPrecompiledContract.CacheEvent(
                  name, AbstractPrecompiledContract.CacheMetric.FALSE_POSITIVE));
        }
      } else {
        cacheEventConsumer.accept(
            new AbstractPrecompiledContract.CacheEvent(
                name, AbstractPrecompiledContract.CacheMetric.MISS));
      }
    }

    final byte[] result = new byte[LibGnarkEIP2537.EIP2537_PREALLOCATE_FOR_RESULT_BYTES];
    final byte[] error = new byte[LibGnarkEIP2537.EIP2537_PREALLOCATE_FOR_ERROR_BYTES];

    final IntByReference o_len =
        new IntByReference(LibGnarkEIP2537.EIP2537_PREALLOCATE_FOR_RESULT_BYTES);
    final IntByReference err_len =
        new IntByReference(LibGnarkEIP2537.EIP2537_PREALLOCATE_FOR_ERROR_BYTES);

    final int inputSize = Math.min(inputLimit, input.size());
    final int errorNo =
        LibGnarkEIP2537.eip2537_perform_operation(
            operationId,
            input.slice(0, inputSize).toArrayUnsafe(),
            inputSize,
            result,
            o_len,
            error,
            err_len);

    if (errorNo == 0) {
      res =
          new PrecompileInputResultTuple(
              enableResultCaching ? cachedInput.copy() : input,
              PrecompileContractResult.success(Bytes.wrap(result, 0, o_len.getValue())));
    } else {
      final String errorMessage = new String(error, 0, err_len.getValue(), UTF_8);
      messageFrame.setRevertReason(Bytes.wrap(error, 0, err_len.getValue()));
      LOG.trace("Error executing precompiled contract {}: '{}'", name, errorMessage);
      res =
          new PrecompileInputResultTuple(
              enableResultCaching ? cachedInput.copy() : input,
              PrecompileContractResult.halt(
                  null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR)));
    }

    if (enableResultCaching && cacheKey != null) {
      getCache().put(cacheKey, res);
    }
    return res.cachedResult();
  }

  /**
   * Gets G1 MSM discount.
   *
   * @param k the k
   * @return the discount
   */
  protected int getG1Discount(final int k) {
    // `k * multiplication_cost * discount / multiplier` where `multiplier = 1000`
    // multiplication_cost and multiplier are folded into one constant as a long and placed first to
    // prevent int32 overflow
    // there was a table prepared for discount in case of k <= 128 points in the multiexponentiation
    // with a discount cup max_discount for k > 128.

    if (k >= G1_DISCOUNT_TABLE.length) {
      return G1_MAX_DISCOUNT;
    }
    return G1_DISCOUNT_TABLE[k];
  }

  /**
   * Gets G2 MSM discount.
   *
   * @param k the k
   * @return the discount
   */
  protected int getG2Discount(final int k) {
    // `k * multiplication_cost * discount / multiplier` where `multiplier = 1000`
    // multiplication_cost and multiplier are folded into one constant as a long and placed first to
    // prevent int32 overflow
    // there was a table prepared for discount in case of k <= 128 points in the multiexponentiation
    // with a discount cup max_discount for k > 128.

    if (k >= G2_DISCOUNT_TABLE.length) {
      return G2_MAX_DISCOUNT;
    }
    return G2_DISCOUNT_TABLE[k];
  }

  /**
   * Enable or disable precompile result caching.
   *
   * @param enablePrecompileCaching boolean indicating whether to cache precompile results
   */
  public static void setPrecompileCaching(final boolean enablePrecompileCaching) {
    enableResultCaching = enablePrecompileCaching;
  }

  /**
   * get the presompile-specific cache.
   *
   * @return precompile cache.
   */
  protected abstract Cache<Integer, PrecompileInputResultTuple> getCache();
}
