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
package org.hyperledger.besu.services.pipeline;

import static java.util.concurrent.CompletableFuture.completedFuture;

import org.hyperledger.besu.services.pipeline.exception.AsyncOperationException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncOperationProcessor<I, O> implements Processor<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncOperationProcessor.class);
  private final Function<I, CompletableFuture<O>> processor;
  private final List<CompletableFuture<O>> inProgress;
  private CompletableFuture<?> nextOutputAvailableFuture = completedFuture(null);
  private final boolean preserveOrder;
  private final int maxConcurrency;

  public AsyncOperationProcessor(
      final Function<I, CompletableFuture<O>> processor,
      final int maxConcurrency,
      final boolean preserveOrder) {
    this.processor = processor;
    this.maxConcurrency = maxConcurrency;
    this.inProgress = new ArrayList<>(maxConcurrency);
    this.preserveOrder = preserveOrder;
  }

  @Override
  public void processNextInput(final ReadPipe<I> inputPipe, final WritePipe<O> outputPipe) {
    if (inProgress.size() < maxConcurrency) {
      final I value = inputPipe.get();
      if (value != null) {
        final CompletableFuture<O> future = processor.apply(value);
        // When the future completes, interrupt so if we're waiting for new input we wake up and
        // schedule the output.
        final Thread stageThread = Thread.currentThread();
        inProgress.add(future);
        updateNextOutputAvailableFuture();
        future.whenComplete((result, error) -> stageThread.interrupt());
      }
      outputCompletedTasks(outputPipe);
    } else {
      outputNextCompletedTask(outputPipe);
    }
  }

  @Override
  public boolean attemptFinalization(final WritePipe<O> outputPipe) {
    outputNextCompletedTask(outputPipe);
    return inProgress.isEmpty();
  }

  @Override
  public void abort() {
    inProgress.forEach(future -> future.cancel(true));
  }

  private void outputNextCompletedTask(final WritePipe<O> outputPipe) {
    try {
      waitForAnyFutureToComplete();
      outputCompletedTasks(outputPipe);
    } catch (final InterruptedException e) {
      LOG.atTrace()
          .setMessage("Interrupted while waiting for processing to complete: Message=({})")
          .addArgument(e.getMessage())
          .setCause(e)
          .log();
    } catch (final ExecutionException e) {
      throw new AsyncOperationException("Async operation failed. " + e.getMessage(), e);
    } catch (final TimeoutException e) {
      // Ignore and go back around the loop.
    }
  }

  private void waitForAnyFutureToComplete()
      throws InterruptedException, ExecutionException, TimeoutException {
    nextOutputAvailableFuture.get(1, TimeUnit.SECONDS);
  }

  private void outputCompletedTasks(final WritePipe<O> outputPipe) {
    boolean inProgressChanged = false;
    boolean outputInThisPass;
    // A task that completes interrupts this thread, so that it stops waiting for input and outputs
    // the task. Putting a result into the output pipe takes that interrupt away. So after a pass
    // that put something, the tasks are looked at again: one that completed meanwhile, behind the
    // point the pass had reached, would otherwise stay here until the next input arrives, which is
    // never if the rest of the pipeline waits for that very task.
    do {
      outputInThisPass = false;
      for (final Iterator<CompletableFuture<O>> i = inProgress.iterator(); i.hasNext(); ) {
        final CompletableFuture<O> process = i.next();
        final O result = process.getNow(null);
        if (result != null) {
          outputInThisPass = true;
          outputPipe.put(result);
          i.remove();
        } else if (preserveOrder) {
          break;
        }
      }
      inProgressChanged |= outputInThisPass;
    } while (outputInThisPass);
    if (inProgressChanged) {
      updateNextOutputAvailableFuture();
    }
  }

  /**
   * CompletableFuture.anyOf adds a completion handler to every future its passed so if we call it
   * too often we can quickly wind up with thousands of completion handlers which take a long time
   * to iterate through and notify. So only create it when the futures it covers have actually
   * changed.
   */
  @SuppressWarnings("rawtypes")
  private void updateNextOutputAvailableFuture() {
    if (preserveOrder) {
      nextOutputAvailableFuture = inProgress.isEmpty() ? completedFuture(null) : inProgress.get(0);
    } else {
      nextOutputAvailableFuture =
          CompletableFuture.anyOf(inProgress.toArray(new CompletableFuture[0]));
    }
  }
}
