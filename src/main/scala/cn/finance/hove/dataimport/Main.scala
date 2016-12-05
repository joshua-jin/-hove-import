package cn.finance.hove.dataimport

import cn.finance.hove.dataimport.common.CommonDataImport
import cn.finance.hove.dataimport.executor.{ActiveGambleAppCountExecutor, CollectionPhoneExecutor, OnlineGambleUrlExecutor, UserLoanAppExecutor}
import java.io.File

object Main {
  def main(args: Array[String]): Unit = {
    args match {
      case Array("user-online-gambling-url", filePrefix: String, threadCount: String) =>
        CommonDataImport.importData(filePrefix, threadCount)(provider = (tc: Int, tb: Int, fp: String) => {
          new OnlineGambleUrlExecutor(tc, tb, fp)
        })

      case Array("user-loan-app-count", filePrefix: String, threadCount: String) =>
        CommonDataImport.importData(filePrefix, threadCount)(provider = (tc: Int, tb: Int, fp: String) => {
          new UserLoanAppExecutor(tc, tb, fp)
        })

      case Array("white-list-sms", filePrefix: String, threadCount: String) =>
        WhiteListSMSDataImport.importData(filePrefix, threadCount)

      case Array("white-list-score", filePrefix: String, threadCount: String) =>
        WhiteListScoreDataImport.importData(filePrefix, threadCount)

      case Array("p2p-phone-list", fileName: String) => P2pPhoneListDataImport.importData(fileName)

      case Array("user-active-gamble-app", filePrefix: String, threadCount: String) =>
        CommonDataImport.importData(filePrefix, threadCount)(provider = (tc: Int, tb: Int, fp: String) => {
          new ActiveGambleAppCountExecutor(tc, tb, fp)
        })

      case Array("external-blacklist", file:String) =>
        BlackListImport.doImport("external-blacklist", new File(file))

      case Array("dailianmeng-blacklist",file:String) =>
        BlackListImport.doImport("dailianmeng-blacklist", new File(file))

      case Array("collection-phone", filePrefix: String, threadCount: String) =>
        CommonDataImport.importData(filePrefix, threadCount)(provider = (tc: Int, tb: Int, fp: String) => {
          CollectionPhoneExecutor(tc, tb, fp)
        })

      case Array("device-collection-rate", file:String) => DeviceCollectionCallingRateImport.doImport(new File(file))

      case _ =>
        println("")
        println("You can use \"hove-import\" like this:")
        println("-----------------------------------------------------------------------------------------------------")
        println("user-online-gambling-url <filePrefix> <threadCount>")
        println("user-loan-app-count <filePrefix> <threadCount>")
        println("white-list-sms <filePrefix> <threadCount>")
        println("white-list-score <filePrefix> <threadCount>")
        println("user-active-gamble-app <filePrefix> <threadCount>")
        println("collection-phone <filePrefix> <threadCount>")
        println("p2p-phone-list <fileName>")
        println("external-blacklist <fileName>")
        println("dailianmeng-blacklist <fileName>")
        println("device-collection-rate <fileName>")
    }
  }
}
