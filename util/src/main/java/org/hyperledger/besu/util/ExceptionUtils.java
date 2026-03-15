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
package org.hyperledger.besu.util;

import com.google.common.base.Throwables;
import org.jspecify.annotations.Nullable;

/** The Exception utils. */
public class ExceptionUtils {

  private ExceptionUtils() {}

  /**
   * Returns the root cause of an exception
   *
   * @param throwable the throwable whose root cause we want to find
   * @return The root cause
   */
  public static @Nullable Throwable rootCause(final @Nullable Throwable throwable) {
    return throwable != null ? Throwables.getRootCause(throwable) : null;
  }
}
