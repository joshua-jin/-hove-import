package cn.finance.hove.dataimport

object Main {
  def main(args: Array[String]): Unit = {
    args match {
      case Array("user-360-var", filePrefix: String, threadCount: String) => {
        Pre360DataImport.importData(filePrefix, threadCount)
      }
      case Array("white-list-sms", filePrefix: String, threadCount: String) => {
        WhiteListSMSDataImport.importData(filePrefix, threadCount)
      }
      case Array("white-list-score", filePrefix: String, threadCount: String) => {
        WhiteListScoreDataImport.importData(filePrefix, threadCount)
      }
      case Array("p2p-phone-list", fileName: String) => {
        P2pPhoneListDataImport.importData(fileName)
      }
      case _ => {
        println("Arguments is not correct...")
      }
    }
  }
}
