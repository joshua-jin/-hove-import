package cn.finance.hove.dataimport

import java.io.File
import java.util
import java.util.concurrent.{ExecutorService, Executors, Future, TimeUnit}

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import com.roundeights.hasher.Implicits._

import scala.util.matching._
import scala.io.Source
import scala.util.matching.Regex.Match

object BlackNameImport {

  def doImport(tableName:String,file : File):Unit = {
     val table = HBaseService.getConnection.getBufferedMutator(TableName.valueOf(tableName))
     val fCommoncolumn = Bytes.toBytes("f")


     val nameColumn = Bytes.toBytes("name")
     val idNoColumn = Bytes.toBytes("idNo")
     val mobileColumn = Bytes.toBytes("mobile")
     val uncredited = Bytes.toBytes("uncredited")

    val sourceFile = Source.fromFile(file)
    val putList = new util.ArrayList[Put]()
    var putActionIndex = 0
    val fileName =file.getName
    var noDataCount = 0

    val idNoRegex = new Regex("[\\d\\*xX]{15,18}")
    val mobileRegex = new Regex("1[\\d\\*]{10}")

    println(s"begin to import $fileName")

    def importIdNumRowkey(name: String,mobile: Match, nameIdNoRowKey: Array[Byte]): Boolean = {
      val p = new Put(nameIdNoRowKey)
      p.add(fCommoncolumn, nameColumn, Bytes.toBytes(name))
      p.add(fCommoncolumn, idNoColumn, Bytes.toBytes(mobile.toString()))
      p.add(fCommoncolumn, uncredited, Bytes.toBytes(true))
      putList.add(p)
    }

    def importMobileRowkey(name: String, mobile: Match, nameMobileRowKey: Array[Byte]): Boolean = {
      val p = new Put(nameMobileRowKey)
      p.add(fCommoncolumn, nameColumn, Bytes.toBytes(name))
      p.add(fCommoncolumn, mobileColumn, Bytes.toBytes(mobile.toString()))
      p.add(fCommoncolumn, uncredited, Bytes.toBytes(true))
      putList.add(p)
    }

    def isValidData(name: String, idNum: Match, mobileNum: Match): Boolean = {
      name != "" && (mobileNum != null || idNum != null)
    }

    sourceFile.getLines().foreach(line => {
      val dataArray = line.split(",")
      val name = dataArray(0)
      val idNo = dataArray(1)
      val mobile = dataArray(2)


      val idNumRegex: Iterator[Match] = idNoRegex.findAllMatchIn(idNo)
      var idNum:Match = null
      if (idNumRegex.hasNext){
        idNum = idNumRegex.next()
      }

      val mobileNumRegex: Iterator[Match] = mobileRegex.findAllMatchIn(mobile)
      var mobileNum:Match = null
      if (mobileNumRegex.hasNext){
        mobileNum = mobileNumRegex.next()
      }

      if (isValidData(name, idNum, mobileNum)){
        val nameDataKey = name.sha1.bytes
        if (idNum != null  ){
          val idNoDataKey = idNum.toString().sha1.bytes
          val nameIdNumRowKey = nameDataKey ++ idNoDataKey
          importIdNumRowkey(name, idNum, nameIdNumRowKey)
        }
        if (mobileNum != null) {
          val mobileDateKey = mobileNum.toString().sha1.bytes
          val nameMobileNumRowkey = nameDataKey ++ mobileDateKey
          importMobileRowkey(name, mobileNum, nameMobileNumRowkey)
        }
        if (putActionIndex > 0 && putActionIndex % 500 == 0) {
          table.mutate(putList) //Batch import
          table.flush()
          putList.clear()
        }
        putActionIndex += 1
      } else {
        noDataCount += 1
      }
})

    if (!putList.isEmpty){
      table.mutate(putList)
      table.flush()
    }
    sourceFile.close()
    println(s"$fileName: $putActionIndex --- finished")
    println(s"$fileName: $noDataCount --- no data count")
  }
}

