package cn.finance.hove.dataimport

import java.net.URL

import scala.io.Source

object MainTest {
  def main(args: Array[String]): Unit = {
    val source =
        Source.fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("test.csv"))
    source.getLines().foreach(line => {

    })
    source.close()
    val aURL = new URL("http://example.com:80/docs/books/tutorial"
      + "/index.html?name=networking#DOWNLOADING");
    println(aURL.getHost())
  }
}
