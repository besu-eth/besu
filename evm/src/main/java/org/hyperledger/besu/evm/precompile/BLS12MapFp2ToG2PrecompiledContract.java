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

import com.github.benmanes.caffeine.cache.Cache;
import org.apache.tuweni.bytes.Bytes;

/** The BLS12MapFp2ToG2 precompiled contract. */
public class BLS12MapFp2ToG2PrecompiledContract extends AbstractBLS12PrecompiledContract {

  private static final int PARAMETER_LENGTH = 128;
  private static final Cache<Integer, PrecompileInputResultTuple> mapfp2g2Cache =
      AbstractPrecompiledContract.resultCacheBuilder().build();

  /** Instantiates a new BLS12MapFp2ToG2 precompiled contract. */
  BLS12MapFp2ToG2PrecompiledContract() {
    super(
        "BLS12_MAP_FP2_TO_G2",
        LibGnarkEIP2537.BLS12_MAP_FP2_TO_G2_OPERATION_SHIM_VALUE,
        PARAMETER_LENGTH);
  }

  @Override
  public long gasRequirement(final Bytes input) {
    return 23_800L;
  }

  @Override
  protected Cache<Integer, PrecompileInputResultTuple> getCache() {
    return mapfp2g2Cache;
  }
}
