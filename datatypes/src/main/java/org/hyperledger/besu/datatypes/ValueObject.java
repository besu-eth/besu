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
package org.hyperledger.besu.datatypes;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a value object, which is a candidate to become a {@code value class} once
 * Project Valhalla (JEP 401) is available.
 *
 * <p>A class carrying this annotation commits to the following contract, so that the migration to a
 * value class is source and behaviour compatible:
 *
 * <ul>
 *   <li>the class is {@code final} and all its fields are {@code final};
 *   <li>{@link Object#equals(Object)} and {@link Object#hashCode()} are based only on the state of
 *       the object, never on its identity;
 *   <li>instances are created only through static factory methods, which are free to return a
 *       shared instance instead of a new one;
 *   <li>clients must not rely on identity: no {@code ==} comparison, no synchronization and no use
 *       as a key of identity based collections.
 * </ul>
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
public @interface ValueObject {}
