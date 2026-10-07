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
package org.hyperledger.besu.ethereum.slothashing;

import static net.bytebuddy.matcher.ElementMatchers.is;
import static net.bytebuddy.matcher.ElementMatchers.named;

import org.hyperledger.besu.crypto.Hash;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import org.apache.tuweni.bytes.Bytes;

/** Counts keccak256 computations, on every thread, by instrumenting {@link Hash#keccak256}. */
public final class KeccakCounter {

  /** Every keccak256. */
  public static final LongAdder ALL = new LongAdder();

  /** keccak256 of a 32-byte input, which is how storage slot keys are hashed. */
  public static final LongAdder WORD_INPUTS = new LongAdder();

  /** The 32-byte inputs hashed while {@link #distinctWordInputs} runs, null otherwise. */
  public static volatile Set<Bytes> recordedWordInputs;

  private static boolean installed;

  private KeccakCounter() {}

  /** Instruments {@link Hash#keccak256}; needs a JVM that allows attaching an agent. */
  public static synchronized void install() {
    if (installed) {
      return;
    }
    ByteBuddyAgent.install();
    new AgentBuilder.Default()
        .disableClassFormatChanges()
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .type(is(Hash.class))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(Advice.to(CountKeccak.class).on(named("keccak256"))))
        .installOnByteBuddyAgent();
    final long before = ALL.sum();
    Hash.keccak256(Bytes.EMPTY);
    if (ALL.sum() == before) {
      throw new IllegalStateException("keccak256 is not instrumented");
    }
    installed = true;
  }

  /** Runs {@code action} and returns how many different 32-byte inputs it hashed. */
  public static int distinctWordInputs(final Runnable action) {
    recordedWordInputs = ConcurrentHashMap.newKeySet();
    try {
      action.run();
      return recordedWordInputs.size();
    } finally {
      recordedWordInputs = null;
    }
  }

  /** Inlined at the start of {@link Hash#keccak256}. */
  public static final class CountKeccak {

    private CountKeccak() {}

    @Advice.OnMethodEnter
    static void enter(@Advice.Argument(0) final Bytes input) {
      ALL.increment();
      if (input.size() == 32) {
        WORD_INPUTS.increment();
        final Set<Bytes> recorded = recordedWordInputs;
        if (recorded != null) {
          recorded.add(input.copy());
        }
      }
    }
  }
}
