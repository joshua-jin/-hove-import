package cn.finance.hove.dataimport

import java.io.File

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes._
import com.roundeights.hasher.Implicits._
import scala.language.postfixOps

import scala.io.Source

object P2pPhoneListDataImport {
  private val table = HBaseService.getConnection.getTable(TableName.valueOf("p2p-phone-list"))
  private val fCommonColumn = toBytes("f")
  private val mobileColumn = toBytes("mobile")

  def importData(fileName: String): Unit = {
    val file = new File(fileName)
    val source = Source.fromFile(file)
    var countIndex = 0

    val putsList = new java.util.ArrayList[Put]()

    source.getLines().foreach(line => {
      if (line.trim != "") {
        val p = new Put(line.trim.sha1.bytes)
        p.addColumn(fCommonColumn, mobileColumn, toBytes(line.trim))
        putsList.add(p)
        countIndex = countIndex + 1
      }
      if (countIndex >= 500) {
        table.put(putsList)
        countIndex = 0
        putsList.clear()
      }
    })
    if (putsList.size() > 0) {
      table.put(putsList)
    }
    table.close()
  }
}
