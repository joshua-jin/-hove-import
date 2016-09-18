package cn.finance.hove.dataimport

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory._
import org.apache.hadoop.hbase.client.Connection

object HBaseService {
  private val configuration = HBaseConfiguration.create()
  if (System.getProperty("env") == "prod") {
    configuration.set("hbase.master", "10.209.12.110:60000")
    configuration.set("hbase.zookeeper.quorum",
      "10.209.12.120,10.209.12.121,10.209.12.122,10.209.12.123,10.209.12.124")
  }
  val conn: Connection = createConnection(configuration)

  def getConnection: Connection = {
    conn
  }
}
