package cn.finance.hove.dataimport

import java.io.File
import java.util

import com.roundeights.hasher.Implicits._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.io.Source

object DeviceCollectionCallingRateImport {

  val tableName = "device-collection-calling-rate"
  val fCommonColumn = Bytes.toBytes("f")

  // column family 'f': 'imei'	IMEI号
  // column family 'f': 'collect_call_cnt_t_d'	催收电话机构数
  // column family 'f': 'collect_call_cnt_bank_d'	银行催收电话机构数
  // column family 'f': 'collect_call_cnt_nbank_d'	非银行催收电话机构数
  // column family 'f': 'collect_call_cnt_prof_d'	专业机构催收电话机构数
  // column family 'f': 'collect_call_time_t_d'	催收电话次数
  // column family 'f': 'collect_call_time_bank_d'	银行催收电话次数
  // column family 'f': 'collect_call_time_nbank_d'	非银行催收电话次数
  // column family 'f': 'collect_call_time_prof_d'	专业机构催收电话次数
  def doImport(file:File): Unit = {
    val table = HBaseService.getConnection.getBufferedMutator(TableName.valueOf(tableName))

    val imeiColumn = Bytes.toBytes("imei")
    val collectionUnitColumn = Bytes.toBytes("collect_call_cnt_t_d")
    val collectionBankColumn = Bytes.toBytes("collect_call_cnt_bank_d")
    val nonbankColumn = Bytes.toBytes("collect_call_cnt_nbank_d")
    val proUnitColumn = Bytes.toBytes("collect_call_cnt_prof_d")
    val collectionTimesColumn = Bytes.toBytes("collect_call_time_t_d")
    val bankCollectionTimesColumn = Bytes.toBytes("collect_call_time_bank_d")
    val nonbankCollectionTimesColumn = Bytes.toBytes("collect_call_time_nbank_d")
    val proCollectionTimesColumn = Bytes.toBytes("collect_call_time_prof_d")

    println(s"begin to import ${file.getName} ")
    var putActionIndex = 0
    var totalPutCount = 0
    val putList = new util.ArrayList[Put]()
    val sourceFile = Source.fromFile(file)
    sourceFile.getLines().foreach(line => {
      val put = createPutByLine(line)
      putList.add(put)
      putActionIndex += 1
      totalPutCount += 1
      if (putActionIndex >= 1000) {
        mutateTable()
      }
    })
    if (!putList.isEmpty) {
      mutateTable()
    }

    sourceFile.close()
    println(s"${file.getName}: $totalPutCount records put --- finished")

    def mutateTable(): Unit = {
      table.mutate(putList) //Batch import
      println(s"${putList.size()} records put...")
      table.flush()
      putActionIndex = 0
      putList.clear()
    }


    def createPutByLine(line: String) :Put = {
      val data = line.split("\t")
      val imei=data(0)
      val collectionUnit = data(1)
      val collectionBank = data(2)
      val nonbank = data(3)
      val proUnit = data(4)
      val collectionTimes = data(5)
      val bankCollectionTimes = data(6)
      val nonbankCollectionTimes = data(7)
      val proCollectionTimes = data(8)

      val put = new Put(imei.sha1.bytes)
      put.addColumn(fCommonColumn, imeiColumn, Bytes.toBytes(imei))
      put.addColumn(fCommonColumn, collectionUnitColumn, Bytes.toBytes(collectionUnit))
      put.addColumn(fCommonColumn, collectionBankColumn, Bytes.toBytes(collectionBank))
      put.addColumn(fCommonColumn, nonbankColumn, Bytes.toBytes(nonbank))
      put.addColumn(fCommonColumn, proUnitColumn, Bytes.toBytes(proUnit))
      put.addColumn(fCommonColumn, collectionTimesColumn, Bytes.toBytes(collectionTimes))
      put.addColumn(fCommonColumn, bankCollectionTimesColumn, Bytes.toBytes(bankCollectionTimes))
      put.addColumn(fCommonColumn, nonbankCollectionTimesColumn, Bytes.toBytes(nonbankCollectionTimes))
      put.addColumn(fCommonColumn, proCollectionTimesColumn, Bytes.toBytes(proCollectionTimes))

      put
    }

  }
}

