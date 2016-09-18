package cn.finance.hove.dataimport

object HBaseConnectionTest {
  def testScan: Unit = {
    val conn = HBaseService.getConnection
  }
}
