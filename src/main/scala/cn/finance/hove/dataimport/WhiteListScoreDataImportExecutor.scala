package cn.finance.hove.dataimport

import java.io.File
import java.util.concurrent.{ExecutorService, Executors, Future, TimeUnit}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes._
import com.roundeights.hasher.Implicits._
import scala.language.postfixOps

import scala.io.Source

class WhiteListScoreDataImportExecutor(val taskIndex: Int, val threadBound: Int,
                                       val filePrefix: String) extends Runnable {

  private val table = HBaseService.getConnection.getBufferedMutator(TableName.valueOf("white-list-score"))
  private val fCommonColumn = toBytes("f")
  private val flagColumn = toBytes("flag")
  private val typeColumn = toBytes("type")
  private val validPeriodColumn = toBytes("valid-period")
  private val batchNoColumn = toBytes("batch-no")
  private val score1Column = toBytes("s1-val")
  private val scoreGroup1Column = toBytes("s1-grp")
  private val score2Column = toBytes("s2-val")
  private val scoreGroup2Column = toBytes("s2-grp")

  def doImport(file: File): Unit = {
    val sourceFile = Source.fromFile(file)
    val putsList = new java.util.ArrayList[Put]()
    var putsActionIndex = 0
    var importCount = 0
    val fileName = file.getName
    var noDataCount = 0
    println(s"begin to import: $fileName")

    sourceFile.getLines().foreach(line => {
      val lineData = line.split("\\t", -1)
      if (lineData.length == 9) {
        val imei = lineData(0).trim
        val imsi = lineData(1).trim
        val whiteListType = lineData(2).trim
        val expirationDate = lineData(3).trim
        val whiteListBatch = lineData(4).trim
        val score1 = lineData(5).trim
        val scoreGroup1 = lineData(6).trim
        val score2 = lineData(7).trim
        val scoreGroup2 = lineData(8).trim

        if (imei != "" || imsi != "") {
          val p = new Put((imei + imsi).sha1.bytes)
          p.addColumn(fCommonColumn, flagColumn, toBytes("true"))
          p.addColumn(fCommonColumn, typeColumn, toBytes(whiteListType))
          p.addColumn(fCommonColumn, validPeriodColumn, toBytes(expirationDate))
          p.addColumn(fCommonColumn, batchNoColumn, toBytes(whiteListBatch))
          p.addColumn(fCommonColumn, score1Column, toBytes(score1))
          p.addColumn(fCommonColumn, scoreGroup1Column, toBytes(scoreGroup1))
          p.addColumn(fCommonColumn, score2Column, toBytes(score2))
          p.addColumn(fCommonColumn, scoreGroup2Column, toBytes(scoreGroup2))
          putsList.add(p)
        }

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
