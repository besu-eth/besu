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
package org.hyperledger.besu.ethereum.trie.pathbased.common;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;

/**
 * A {@link PathBasedValue} that resolves {@link #getPrior()} and {@link #getUpdated()} on first
 * access via suppliers. When {@link #isPriorKnownAbsent()} is true, {@link #getPrior()} returns
 * {@code null} without invoking the prior supplier.
 */
public class PathBasedLazy<T> extends PathBasedValue<T> {

  private final boolean priorKnownAbsent;
  private Supplier<T> priorSupplier;
  private Supplier<T> updatedSupplier;

  public PathBasedLazy(final Supplier<T> priorLoader, final Supplier<T> updatedLoader) {
    super(null, null);
    this.priorKnownAbsent = false;
    this.priorSupplier = Suppliers.memoize(priorLoader::get);
    this.updatedSupplier = Suppliers.memoize(updatedLoader::get);
  }

  private PathBasedLazy(final Supplier<T> updatedLoader) {
    super(null, null);
    this.priorKnownAbsent = true;
    this.priorSupplier = null;
    this.updatedSupplier = Suppliers.memoize(updatedLoader::get);
  }

  public static <T> PathBasedLazy<T> withPriorKnownAbsent(final Supplier<T> updatedLoader) {
    return new PathBasedLazy<>(updatedLoader);
  }

  public boolean isPriorKnownAbsent() {
    return priorKnownAbsent;
  }

  @Override
  public T getPrior() {
    if (priorKnownAbsent) {
      return null;
    }
    if (priorSupplier != null) {
      super.setPrior(priorSupplier.get());
      priorSupplier = null;
    }
    return super.getPrior();
  }

  @Override
  public PathBasedValue<T> setPrior(final T prior) {
    priorSupplier = null;
    return super.setPrior(prior);
  }

  @Override
  public T getUpdated() {
    if (updatedSupplier != null) {
      super.setUpdated(updatedSupplier.get());
      updatedSupplier = null;
    }
    return super.getUpdated();
  }

  @Override
  public PathBasedValue<T> setUpdated(final T updated) {
    updatedSupplier = null;
    return super.setUpdated(updated);
  }

  public PathBasedValue<T> materialize() {
    return new PathBasedValue<>(
        getPrior(), getUpdated(), isLastStepCleared(), isClearedAtLeastOnce());
  }
}
