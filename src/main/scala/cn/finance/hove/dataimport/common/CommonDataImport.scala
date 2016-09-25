package cn.finance.hove.dataimport.common

import java.util
import java.util.concurrent.{ExecutorService, Executors, Future, TimeUnit}

import cn.finance.hove.dataimport.executor.BasicExecutor

object CommonDataImport {
  def importData(filePrefix: String, threadCount: String)(provider: (Int, Int, String) => BasicExecutor): Unit = {
    val fileIndexBound = threadCount.toInt
    val exs: ExecutorService = Executors.newFixedThreadPool(fileIndexBound)
    val futures = new util.ArrayList[Future[_]]()
    for (i <- 0 until fileIndexBound) {
      futures.add(exs.submit(provider(i, fileIndexBound, filePrefix)))
    }

    for (i <- 0 until futures.size()) {
      futures.get(i).get(1, TimeUnit.DAYS)
    }
    exs.shutdown()
  }
}
