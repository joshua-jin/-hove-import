package cn.finance.hove.dataimport

import com.roundeights.hasher.Implicits._
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.util.Bytes

import scala.language.postfixOps

object MainTest {
  def main(args: Array[String]): Unit = {
    val md5Bytes = "hello".md5.bytes
    println(Hex.encodeHexString(md5Bytes))

    val md5Str = "5d41402abc4b2a76b9719d911017c592"
    println(md5Bytes sameElements Hex.decodeHex(md5Str.toCharArray))

    val b = Hex.decodeHex(md5Str.toCharArray)
    println(b.mkString(","))
    println(md5Bytes.mkString(","))

    val imei = "867323026308799"
    println(imei.md5.hex)
    val imeiMD5Str = "1c6517a58d241a72024199cf52010cd3"
    println(imei.md5.hex == imeiMD5Str)
    println(Bytes.toStringBinary(Hex.decodeHex(imeiMD5Str.toCharArray)))
  }
}
