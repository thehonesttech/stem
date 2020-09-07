package org.stem.benchmarks

import org.openjdk.jmh.annotations.Benchmark

object TestBenchmark {
  @Benchmark
  def endToEnd = {
    println("ciao")
  }
}
