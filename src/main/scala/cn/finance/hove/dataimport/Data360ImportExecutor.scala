package cn.finance.hove.dataimport

import java.io.File
import java.util

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import com.roundeights.hasher.Implicits._
import scala.language.postfixOps

import scala.io.Source

class Data360ImportExecutor(val taskIndex: Int, val threadBound: Int, val filePrefix: String) extends Runnable {
  private val table = HBaseService.getConnection.getBufferedMutator(TableName.valueOf("user-360-var"))
  private val fCommonColumn = Bytes.toBytes("f")
  private val gambleColumn = Bytes.toBytes("is-online-gamble")
  private val activeAppColumn = Bytes.toBytes("active-loan-app")

  def doImport(file: File): Unit = {
    val sourceFile = Source.fromFile(file)
    val putsList = new util.ArrayList[Put]()
    var putsActionIndex = 0
    var importCount = 0
    val fileName = file.getName
    var noDataCount = 0
    println(s"begin to import: $fileName")
    sourceFile.getLines().foreach(line => {
      val dataArray = line.split(",", -1)
      val m2ID = dataArray(3)
      if (m2ID != "") {
        val rowKey = m2ID.sha1.bytes
        val gambleUrlCount = dataArray(6)
        val activeAppCount = dataArray(7)
        val p = new Put(rowKey)
        p.add(fCommonColumn, gambleColumn, Bytes.toBytes(gambleUrlCount))
        p.add(fCommonColumn, activeAppColumn, Bytes.toBytes(activeAppCount))
        putsList.add(p)
        if (putsActionIndex >= 500) {
          table.mutate(putsList)
          table.flush()
          putsList.clear()
          putsActionIndex = 0
          importCount += 500
          println(s"$fileName: $importCount")
        } else {
          putsActionIndex += 1
        }
      }
      else {
        noDataCount += 1
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

  override def run(): Unit = {
    var fileIndex = taskIndex
    while (true) {
      val fileSuffix = if (fileIndex < 10) "0" + fileIndex else fileIndex.toString
      val file = new File(filePrefix + fileSuffix)
      if (!file.exists()) {
        return
      }
      doImport(file)
      fileIndex = fileIndex + threadBound
    }
  }
}
