package cn.finance.hove.dataimport.executor

import java.io.File

import org.apache.hadoop.hbase.client.{BufferedMutator, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.io.Source

abstract class BasicExecutor(val taskIndex: Int, val threadBound: Int, val filePrefix: String) extends Runnable {
  protected val fCommonColumn = Bytes.toBytes("f")

  def doImport(file: File): Unit = {
    val sourceFile = Source.fromFile(file)
    val putsList = new java.util.ArrayList[Put]()
    var putsActionIndex = 0
    var importCount = 0
    val fileName = file.getName
    var noDataCount = 0
    val table = getTable()
    println(s"begin to import: $fileName")

    sourceFile.getLines().foreach(line => {
      val (importDataCount, missCount) =
        setupPuts(line, putsList, putsActionIndex, noDataCount)
      putsActionIndex += importDataCount
      noDataCount += missCount
      if (putsActionIndex >= 500) {
        table.mutate(putsList)
        table.flush()
        putsList.clear()
        putsActionIndex = 0
        importCount += 500
        println(s"$fileName: $importCount")
      }
    })
    if (!putsList.isEmpty) {
      table.mutate(putsList)
      table.flush()
      importCount += putsList.size()
    }
    sourceFile.close()
    println(s"$fileName: $importCount --- finished")
    println(s"$fileName: $noDataCount --- no data count")
  }

  def getTable(): BufferedMutator

  def setupPuts(lineData: String, puts: java.util.List[Put],
                         putIndex: Int, noDataCount: Int): (Int, Int)

  override def run(): Unit = {
    var fileIndex = taskIndex
    while (true) {
      val fileSuffix = f"${fileIndex}%03d"
      val file = new File(filePrefix + fileSuffix)
      if (!file.exists()) {
        return
      }
      doImport(file)
      fileIndex = fileIndex + threadBound
    }
  }
}
