/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.util.log4j.plugin;

import java.util.Arrays;
import java.util.Objects;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;
import org.jspecify.annotations.Nullable;

/** Matches a text in the stack trace */
@Plugin(
    name = "StackTraceMatchFilter",
    category = "Core",
    elementType = "filter",
    printObject = true)
public class StackTraceMatchFilter extends AbstractFilter {
  private final String stackContains;
  private final @Nullable String messageEquals;

  private StackTraceMatchFilter(
      final String stackContains,
      final @Nullable String messageEquals,
      final Result onMatch,
      final Result onMismatch) {
    super(onMatch, onMismatch);
    this.stackContains = stackContains;
    this.messageEquals = messageEquals;
  }

  @Override
  public Result filter(
      final Logger logger,
      final Level level,
      final Marker marker,
      final Object msg,
      final Throwable t) {
    return filter(t);
  }

  @Override
  public Result filter(
      final Logger logger,
      final Level level,
      final Marker marker,
      final Message msg,
      final Throwable t) {
    return filter(t);
  }

  @Override
  public Result filter(final LogEvent event) {
    return filter(event.getThrown());
  }

  private Result filter(final Throwable t) {
    if (t != null) {
      return (messageEquals == null || Objects.equals(t.getMessage(), messageEquals))
              && Arrays.stream(t.getStackTrace())
                  .map(StackTraceElement::getClassName)
                  .anyMatch(cn -> cn.contains(stackContains))
          ? onMatch
          : onMismatch;
    }
    return Result.NEUTRAL;
  }

  @Override
  public String toString() {
    return String.valueOf(stackContains);
  }

  /**
   * Create a new builder
   *
   * @return a new builder
   */
  @PluginBuilderFactory
  public static StackTraceMatchFilter.Builder newBuilder() {
    return new StackTraceMatchFilter.Builder();
  }

  /** Builder for StackTraceMatchFilter */
  public static class Builder extends AbstractFilterBuilder<StackTraceMatchFilter.Builder>
      implements org.apache.logging.log4j.core.util.Builder<StackTraceMatchFilter> {

    @PluginBuilderAttribute private @Nullable String stackContains;
    @PluginBuilderAttribute private @Nullable String messageEquals;

    /** Default constructor */
    public Builder() {
      // here to make javadoc happy
    }

    /**
     * Set the string to search in the stack trace
     *
     * @param text the match string
     * @return this builder
     */
    public StackTraceMatchFilter.Builder setStackContains(final @Nullable String text) {
      this.stackContains = text;
      return this;
    }

    /**
     * Set the string to match in the message
     *
     * @param text the match string
     * @return this builder
     */
    public StackTraceMatchFilter.Builder setMessageEquals(final @Nullable String text) {
      this.messageEquals = text;
      return this;
    }

    @Override
    public StackTraceMatchFilter build() {
      final String nonNullStackContains =
          Objects.requireNonNull(stackContains, "stackContains must be provided");

      return new StackTraceMatchFilter(
          nonNullStackContains, this.messageEquals, this.getOnMatch(), this.getOnMismatch());
    }
  }
}
