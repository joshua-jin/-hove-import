package cn.finance.hove.dataimport

import cn.finance.hove.dataimport.common.CommonDataImport
import cn.finance.hove.dataimport.executor.{ActiveGambleAppCountExecutor, OnlineGambleUrlExecutor}

object Main {
  def main(args: Array[String]): Unit = {
    args match {
      case Array("user-online-gambling-url", filePrefix: String, threadCount: String) => {
        CommonDataImport.importData(filePrefix, threadCount)(provider = (tc: Int, tb: Int, fp: String) => {
          new OnlineGambleUrlExecutor(tc, tb, fp)
        })
      }
      case Array("user-loan-app-count", filePrefix: String, threadCount: String) => {
        CommonDataImport.importData(filePrefix, threadCount)(provider = (tc: Int, tb: Int, fp: String) => {
          new OnlineGambleUrlExecutor(tc, tb, fp)
        })
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
      case Array("user-active-gamble-app", filePrefix: String, threadCount: String) => {
        CommonDataImport.importData(filePrefix, threadCount)(provider = (tc: Int, tb: Int, fp: String) => {
          new ActiveGambleAppCountExecutor(tc, tb, fp)
        })
      }
      case _ => {
        println("Arguments is not correct...")
      }
    }
  }
}
