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

/** The BLS12G1 Add precompiled contract. */
public class BLS12G1AddPrecompiledContract extends AbstractBLS12PrecompiledContract {

  private static final int PARAMETER_LENGTH = 256;
  private static final Cache<Integer, PrecompileInputResultTuple> g1AddCache =
      AbstractPrecompiledContract.resultCacheBuilder().build();

  /** Instantiates a new BLS12G1 Add precompiled contract. */
  BLS12G1AddPrecompiledContract() {
    super("BLS12_G1ADD", LibGnarkEIP2537.BLS12_G1ADD_OPERATION_SHIM_VALUE, PARAMETER_LENGTH);
  }

  @Override
  public long gasRequirement(final Bytes input) {
    return 375L;
  }

  @Override
  protected Cache<Integer, PrecompileInputResultTuple> getCache() {
    return g1AddCache;
  }
}
