package cn.finance.hove.dataimport.executor
import java.util

import cn.finance.hove.dataimport.HBaseService
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, Put}
import org.apache.hadoop.hbase.util.Bytes

class OnlineGambleUrlExecutor(taskIndex: Int, threadBound: Int, filePrefix: String)
  extends BasicExecutor(taskIndex, threadBound, filePrefix) {
  override def getTable(): BufferedMutator = {
    HBaseService.getConnection.getBufferedMutator(TableName.valueOf("user-360-var"))
  }

  override def setupPuts(lineData: String, puts: util.List[Put], putIndex: Int, noDataCount: Int): (Int, Int) = {
    val dataArray = lineData.split("\\t")
    if (dataArray.length >= 2) {
      val imeiMD5Str = dataArray(0)
      val urlCount = dataArray(1)
      try {
        val p = new Put(Hex.decodeHex(imeiMD5Str.toCharArray))
        p.add(fCommonColumn, Bytes.toBytes("is-online-gamble"), Bytes.toBytes(urlCount))
        puts.add(p)
        return (1, 0)
      } catch {
        case e: Exception => println(s"""error md5: $imeiMD5Str""")
      }
    }
    (0, 1)
  }
}
