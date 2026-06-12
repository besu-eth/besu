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
package org.hyperledger.besu.evmtool;

import org.hyperledger.besu.util.LogConfigurator;

/** The main entry point for the EVM (Ethereum Virtual Machine) tool. */
public final class EvmTool {

  /** Default constructor for the EvmTool class. */
  public EvmTool() {}

  /**
   * The main entry point for the EVM (Ethereum Virtual Machine) tool.
   *
   * @param args The command line arguments.
   */
  public static void main(final String... args) {
    LogConfigurator.setLevel("", "OFF");
    final EvmToolCommand evmToolCommand = new EvmToolCommand();

    final int exitCode = evmToolCommand.execute(args);
    // Preserve the existing "exit 0 by default" behavior; only force a non-zero exit when a
    // subcommand (e.g. engine-test) reports failures so CI and gradle can detect them.
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }
}
