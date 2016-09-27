package cn.finance.hove.dataimport

import java.io.File
import java.util

import com.roundeights.hasher.Implicits._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.io.Source
import scala.util.matching.Regex.Match
import scala.util.matching._

object BlackListImport {

  def doImport(tableName:String,file : File):Unit = {
     val table = HBaseService.getConnection.getBufferedMutator(TableName.valueOf(tableName))
     val fCommonColumn = Bytes.toBytes("f")

     val nameColumn = Bytes.toBytes("name")
     val idNoColumn = Bytes.toBytes("idNo")
     val mobileColumn = Bytes.toBytes("mobile")
     val uncredited = Bytes.toBytes("uncredited")

    val sourceFile = Source.fromFile(file)
    val putList = new util.ArrayList[Put]()
    var putActionIndex = 0
    var totalPutCount = 0
    var validLineCount = 0
    var invalidLineCount = 0
    val fileName =file.getName

    val idNoRegex = new Regex("[\\d\\*xX]{15,18}")
    val mobileRegex = new Regex("1[\\d\\*]{10}")

    println(s"begin to import $fileName")

    def addNameMobilePut(name: String, mobile: String): Unit  = {
      val rowKey = name.sha1.bytes ++ mobile.sha1.bytes
      val p = new Put(rowKey)
      p.addColumn(fCommonColumn, nameColumn, Bytes.toBytes(name))
      p.addColumn(fCommonColumn, mobileColumn, Bytes.toBytes(mobile))
      p.addColumn(fCommonColumn, uncredited, Bytes.toBytes("true"))
      putList.add(p)
    }

    def addNameIdNumPut(name: String, idNo: String): Unit = {
      val rowKey = name.sha1.bytes ++ idNo.sha1.bytes
      val p = new Put(rowKey)
      p.addColumn(fCommonColumn, nameColumn, Bytes.toBytes(name))
      p.addColumn(fCommonColumn, idNoColumn, Bytes.toBytes(idNo))
      p.addColumn(fCommonColumn, uncredited, Bytes.toBytes("true"))
      putList.add(p)
    }

    def isValidLine(name: String, idNum: String, mobileNum: String): Boolean = {
      name != "" && (mobileNum != null || idNum != null)
    }

    def extractFromRegex(regex:Regex,str:String):String = {
      val matcher: Iterator[Match] = regex.findAllMatchIn(str)
      if (matcher.hasNext){
        return matcher.next().toString()
      }
      return null
    }

    def extractDataFromLine(line:String):(String,String,String) ={
      val dataArray = line.split(",")
      return (dataArray(0), extractFromRegex(idNoRegex,dataArray(1)), extractFromRegex(mobileRegex,dataArray(2)))
    }

    sourceFile.getLines().filter(x =>{ x.split(",").length >= 3}).foreach(line => {
        val (name,idNum,mobileNum) = extractDataFromLine(line)
        if (isValidLine(name, idNum, mobileNum)){
          if (idNum != null  ){
            addNameIdNumPut(name, idNum)
            putActionIndex += 1
            totalPutCount += 1
          }
          if (mobileNum != null) {
            addNameMobilePut(name, mobileNum)
            putActionIndex += 1
            totalPutCount += 1
          }
          if (putActionIndex >= 500) {
            table.mutate(putList) //Batch import
            println(s"${putList.size()} records put...")
            table.flush()
            putActionIndex = 0
            putList.clear()
          }
          validLineCount += 1
        } else {
          invalidLineCount += 1
        }
    })

    if (!putList.isEmpty){
      table.mutate(putList)
      println(s"${putList.size()} records put...")
      table.flush()
    }
    sourceFile.close()
    println(s"$fileName: total $validLineCount lines $totalPutCount records put --- finished")
    println(s"$fileName: $invalidLineCount --- no data count")
  }
}

