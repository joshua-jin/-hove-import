package cn.finance.hove.dataimport

import java.util
import java.util.concurrent.{ExecutorService, Executors, Future, TimeUnit}

object Pre360DataImport {
  def importData(filePrefix: String, threadCount: String): Unit = {
    val fileIndexBound = threadCount.toInt
    val exs: ExecutorService = Executors.newFixedThreadPool(fileIndexBound)
    val futures = new util.ArrayList[Future[_]]()
    for (i <- 0 until fileIndexBound) {
      futures.add(exs.submit(new Data360ImportExecutor(i, fileIndexBound, filePrefix)))
    }

    for (i <- 0 until futures.size()) {
      futures.get(i).get(1, TimeUnit.DAYS)
    }
    exs.shutdown()
  }
}
