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

import org.hyperledger.besu.nativelib.gnark.LibGnarkEIP2537;

import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import org.apache.tuweni.bytes.Bytes;

/** The BLS12_G2 MultiExp precompiled contract. */
public class BLS12G2MultiExpPrecompiledContract extends AbstractBLS12PrecompiledContract {

  private static final int PARAMETER_LENGTH = 288;
  private static final Cache<Integer, PrecompileInputResultTuple> g2MSMCache =
      AbstractPrecompiledContract.resultCacheBuilder()
          .expireAfterWrite(15, TimeUnit.MINUTES) // Evict 15 minutes after each entry is written
          .build();

  /** Instantiates a new BLS12_G2 MultiExp precompiled contract. */
  BLS12G2MultiExpPrecompiledContract() {
    super(
        "BLS12_G2MSM",
        LibGnarkEIP2537.BLS12_G2MULTIEXP_OPERATION_SHIM_VALUE,
        Integer.MAX_VALUE / PARAMETER_LENGTH * PARAMETER_LENGTH);
  }

  @Override
  public long gasRequirement(final Bytes input) {
    final int k = input.size() / PARAMETER_LENGTH;
    return 22_500L * k * getG2Discount(k) / 1000L;
  }

  @Override
  protected Cache<Integer, PrecompileInputResultTuple> getCache() {
    return g2MSMCache;
  }
}
