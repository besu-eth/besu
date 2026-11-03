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
package org.hyperledger.besu.evm.testutils;

import org.hyperledger.besu.evm.internal.EvmConfiguration;

/** EVM configurations for tests that need a particular EVM whatever the default is. */
public final class TestEvmConfigurations {

  /** Runs the frames {@link TestMessageFrameBuilder} and the EVM v1 operation tests build. */
  public static final EvmConfiguration EVM_V1 =
      new EvmConfiguration(
          EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
          EvmConfiguration.DEFAULT.worldUpdaterMode(),
          EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
          false);

  /** Runs the frames the EVM v2 tests build. */
  public static final EvmConfiguration EVM_V2 =
      new EvmConfiguration(
          EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
          EvmConfiguration.DEFAULT.worldUpdaterMode(),
          EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
          true);

  private TestEvmConfigurations() {}
}
