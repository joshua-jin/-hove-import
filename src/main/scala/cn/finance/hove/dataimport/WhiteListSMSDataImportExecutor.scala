package cn.finance.hove.dataimport

import java.io.File
import java.util

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import com.roundeights.hasher.Implicits._
import org.apache.hadoop.hbase.util.Bytes.toBytes

import scala.language.postfixOps
import scala.io.Source

class WhiteListSMSDataImportExecutor(val taskIndex: Int, val threadBound: Int,
                                     val filePrefix: String) extends Runnable {
  private val table = HBaseService.getConnection.getBufferedMutator(TableName.valueOf("white-list-sms"))
  private val fCommonColumn = toBytes("f")
  private val sms6c = toBytes("sms-6mc")
  private val sms1mSalary = toBytes("1m-sms-salary")
  private val sms2mSalary = toBytes("2m-sms-salary")
  private val sms3mSalary = toBytes("3m-sms-salary")
  private val sms4mSalary = toBytes("4m-sms-salary")
  private val sms5mSalary = toBytes("5m-sms-salary")
  private val sms6mSalary = toBytes("6m-sms-salary")
  private val overdueSms30C = toBytes("ov-sms-30dc")
  private val overdueSms90C = toBytes("ov-sms-90dc")

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
      val imei = dataArray(0)
      if (imei != "") {
        val rowKey = imei.sha1.bytes
        val p = new Put(rowKey)
        if ("\\N" != dataArray(1)) {
          p.addColumn(fCommonColumn, sms6c, toBytes(dataArray(1)))
        }
        if ("\\N" != dataArray(2)) {
          p.addColumn(fCommonColumn, sms1mSalary, toBytes(dataArray(2)))
        }
        if ("\\N" != dataArray(3)) {
          p.addColumn(fCommonColumn, sms2mSalary, toBytes(dataArray(3)))
        }
        if ("\\N" != dataArray(4)) {
          p.addColumn(fCommonColumn, sms3mSalary, toBytes(dataArray(4)))
        }
        if ("\\N" != dataArray(5)) {
          p.addColumn(fCommonColumn, sms4mSalary, toBytes(dataArray(5)))
        }
        if ("\\N" != dataArray(6)) {
          p.addColumn(fCommonColumn, sms5mSalary, toBytes(dataArray(6)))
        }
        if ("\\N" != dataArray(7)) {
          p.addColumn(fCommonColumn, sms6mSalary, toBytes(dataArray(7)))
        }
        if ("\\N" != dataArray(8)) {
          p.addColumn(fCommonColumn, overdueSms30C, toBytes(dataArray(8)))
        }
        if ("\\N" != dataArray(9)) {
          p.addColumn(fCommonColumn, overdueSms90C, toBytes(dataArray(9)))
        }
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
