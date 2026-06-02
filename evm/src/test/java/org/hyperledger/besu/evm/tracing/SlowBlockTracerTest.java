package org.hyperledger.besu.evm.tracing;

import org.hyperledger.besu.datatypes.Hash;
import org.junit.jupiter.api.Test;

class SlowBlockTracerTest {

  @Test
  public void shouldLog() {
    SlowBlockTracer slowBlockTracer = new SlowBlockTracer();
    slowBlockTracer.traceStartBlock();
    slowBlockTracer.traceEndBlock(20_000_001L, Hash.EMPTY, 60_000_000L);
  }
}