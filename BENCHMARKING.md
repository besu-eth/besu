# Running JMH Benchmarks in Besu

Besu includes [JMH](https://openjdk.org/projects/code-tools/jmh/) microbenchmarks for performance testing. This guide explains how to build and run them using Gradle.

## 🛠️ Prerequisites

- Java 21+ (ensure `JAVA_HOME` is set)
- Gradle (you can use the wrapper: `./gradlew`)
- Optional: [Async Profiler](https://github.com/jvm-profiling-tools/async-profiler) for low-overhead profiling

---

## 🏃 Running Benchmarks

JMH tasks are defined in various modules such as `ethereum:core`, `ethereum:eth`, and `ethereum:rlp`.

### ▶️  Run All Benchmarks

```bash
./gradlew :ethereum:core:jmh
```

This runs all available benchmarks in the `core` module using default settings.

Gradle won't rerun a task by default with no changes, so to run subsequent times without changes add the gradle `--rerun-tasks` option.

### 🔁 Rerun All Benchmarks

```bash
./gradlew :ethereum:core:jmh --rerun-tasks
```

---

## 🎯 Filter Benchmarks by Name and Case

To run a specific benchmark class, use the `-Pincludes` and/or `-Pexcludes` project properties. To filter by case name, use `-Pcases`:

```bash
./gradlew :ethereum:core:jmh -Pincludes=Mod -Pexcludes=Mul,Add,SMod -Pcases=MOD_256_128,MOD_256_192 --rerun-tasks
```

This uses a regex pattern so other kinds of regexes can be used.

---

## ⚙️ Benchmark Configuration Options

For other configuration options run:

```bash
./gradlew help --task jmh
```

---

## 🔥 Async Profiler Integration (Optional)

To profile benchmarks with [Async Profiler](https://github.com/jvm-profiling-tools/async-profiler):

```bash
./gradlew :ethereum:core:jmh \
  -Pincludes=SomeBenchmark \
  -PasyncProfiler=/path/to/libasyncProfiler.so \
  -PasyncProfilerOptions="output=flamegraph" \
  --rerun-tasks
```

This will generate two html profiling files flame-cpu-forward.html and flame-cpu-reverse.html after the benchmark run.

---

## 🧪 Sample Output

```
Benchmark                                Mode  Cnt  Score   Error  Units
SomeBenchmark.benchmark                  avgt   20  3.893 ± 0.021  ns/op
SomeBenchmark.benchmark                  avgt   20  8.951 ± 0.149  ns/op
```

---

## 📚 References

- [JMH Gradle plugin documentation](https://github.com/melix/jmh-gradle-plugin)
- [JMH Documentation](https://openjdk.org/projects/code-tools/jmh/)
- [Async Profiler GitHub](https://github.com/jvm-profiling-tools/async-profiler)
