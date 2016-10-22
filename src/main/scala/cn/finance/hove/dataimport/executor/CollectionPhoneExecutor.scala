package cn.finance.hove.dataimport.executor
import java.util

import cn.finance.hove.dataimport.HBaseService
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, Put}
import org.apache.hadoop.hbase.util.Bytes

import com.roundeights.hasher.Implicits._
import scala.language.postfixOps

case class CollectionPhoneExecutor(tc: Int, tb: Int, fp: String)
  extends BasicExecutor(tc, tb, fp) {
  override def getTable(): BufferedMutator = {
    HBaseService.getConnection.getBufferedMutator(TableName.valueOf("collection-phone-list"))
  }

  override def setupPuts(lineData: String, puts: util.List[Put], putIndex: Int, noDataCount: Int): (Int, Int) = {
    val dataArray = lineData.split("\\t")
    if (dataArray.length >= 2) {
      val phone = dataArray(0)
      val phoneType = dataArray(1)
      try {
        val p = new Put(phone.sha1.bytes)
        p.add(fCommonColumn, Bytes.toBytes("mobile"), Bytes.toBytes(phone))
        if (phoneType == "非银催收") {
          p.add(fCommonColumn, Bytes.toBytes("is-pro"), Bytes.toBytes("false"))
        } else {
          p.add(fCommonColumn, Bytes.toBytes("is-pro"), Bytes.toBytes("true"))
        }
        puts.add(p)
        return (1, 0)
      } catch {
        case e: Exception => println(s"""error phone: $phone""")
      }
    }
    (0, 1)
  }
}
