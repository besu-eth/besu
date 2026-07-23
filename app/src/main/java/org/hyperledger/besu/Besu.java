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
package org.hyperledger.besu;

import org.hyperledger.besu.cli.BesuCommand;
import org.hyperledger.besu.cli.logging.LoggingBootstrapConfigurator;
import org.hyperledger.besu.components.BesuComponent;
import org.hyperledger.besu.components.DaggerBesuComponent;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.RunLast;

/** Besu bootstrap class. */
public final class Besu {
  /** Default constructor. */
  public Besu() {}

  /**
   * The main entrypoint to Besu application
   *
   * @param args command line arguments.
   */
  public static void main(final String... args) {
    setupLogging(args);
    final BesuComponent besuComponent = DaggerBesuComponent.create();
    final BesuCommand besuCommand = besuComponent.getBesuCommand();
    int exitCode =
        besuCommand.parse(
            new RunLast(),
            besuCommand.parameterExceptionHandler(),
            besuCommand.executionExceptionHandler(),
            System.in,
            besuComponent,
            args);

    System.exit(exitCode);
  }

  /**
   * a Logger setup for handling any exceptions during the bootstrap process, to indicate to users
   * their CLI configuration had problems.
   *
   * @param args the raw command line arguments, used to resolve the logging format/color settings
   *     before the first logger is created
   */
  public static void setupLogging(final String... args) {
    try {
      LoggingBootstrapConfigurator.configure(args);
    } catch (Throwable t) {
      System.out.printf(
          "Could not resolve logging format/color configuration: %s - %s%n",
          t.getClass().getSimpleName(), t.getMessage());
    }
    try {
      InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    } catch (Throwable t) {
      System.out.printf(
          "Could not set netty log4j logger factory: %s - %s%n",
          t.getClass().getSimpleName(), t.getMessage());
    }
    try {
      System.setProperty(
          "vertx.logger-delegate-factory-class-name",
          "io.vertx.core.logging.Log4j2LogDelegateFactory");
      System.setProperty("log4j.skipJansi", String.valueOf(false));
    } catch (Throwable t) {
      System.out.printf(
          "Could not set logging system property: %s - %s%n",
          t.getClass().getSimpleName(), t.getMessage());
    }
  }

  /**
   * Returns the first logger to be created. This is used to set the default uncaught exception
   *
   * @return Logger
   */
  public static Logger getFirstLogger() {
    final Logger logger = LoggerFactory.getLogger(Besu.class);
    Thread.setDefaultUncaughtExceptionHandler(slf4jExceptionHandler(logger));
    Thread.currentThread().setUncaughtExceptionHandler(slf4jExceptionHandler(logger));

    return logger;
  }

  private static Thread.UncaughtExceptionHandler slf4jExceptionHandler(final Logger logger) {
    return (thread, error) -> {
      if (logger.isErrorEnabled()) {
        logger.error(String.format("Uncaught exception in thread \"%s\"", thread.getName()), error);
      }
    };
  }
}
