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
package org.hyperledger.besu.evm.precompile;

import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Skips native BLS tests on Windows only when the Gnark library is unavailable.
 *
 * <p>Gnark 2.1.0 bundles Linux and macOS libraries but no Windows BLS DLL. Calling its native
 * functions without that DLL throws {@link UnsatisfiedLinkError}, so this condition disables the
 * affected tests before test instances are constructed. Tests run automatically when a Windows
 * native library is available. Linux and macOS tests stay enabled so native-library failures remain
 * visible there.
 *
 * <p>This is a test-only workaround and provides no Windows BLS runtime fallback. See the <a
 * href="https://github.com/besu-eth/besu/issues/11955#issuecomment-6860821992">Windows Gnark
 * discussion</a> for the question about supported binaries and alternatives.
 */
class WindowsBlsNativeCondition implements ExecutionCondition {

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
    return evaluate(OS.WINDOWS.isCurrentOs(), AbstractBLS12PrecompiledContract::isAvailable);
  }

  static ConditionEvaluationResult evaluate(
      final boolean windows, final BooleanSupplier nativeAvailable) {
    if (windows && !nativeAvailable.getAsBoolean()) {
      return ConditionEvaluationResult.disabled(
          "Gnark native BLS library is unavailable on Windows");
    }
    return ConditionEvaluationResult.enabled("Native BLS tests are required on this platform");
  }
}
