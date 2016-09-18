package cn.finance.hove.dataimport

import java.security.MessageDigest

object MessageDigestUtil {
  val digest: MessageDigest = MessageDigest.getInstance("SHA-1")

  def getSHADigest(plainText: Array[Byte]): Array[Byte] = {
    digest.update(plainText)
    return digest.digest(plainText)
  }
}
