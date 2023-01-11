package org.ty.blob

import org.apache.log4j.Logger

import java.io.{BufferedOutputStream, File, FileOutputStream}
import scala.util.Random

object BlobFileWriter {
  val log = Logger.getLogger(this.getClass)
  val size: Int = 400*1024
//  val bytesArray: Array[Byte] = new Array[Byte](size)
  def randomBytes: Array[Byte] = new Array[Byte](size).map(_ => Random.nextInt().toByte)

  def write(bytesArray:Array[Byte], targetFile: File) = {
    val bos: BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(targetFile), size)
    bos.write(bytesArray)
    bos.flush()
    bos.close()
  }

  def batchGenerator(fileCount: Int, targetPath: String, namePrefix: String): Unit = {
    val a = new Array[Int](fileCount).zipWithIndex.map(pair => {
      val fileName: String = namePrefix + pair._2.toString
      val targetFile: File = new File(s"$targetPath/$fileName")
//      write(bytesArray, targetFile)
      write(randomBytes, targetFile)
    }
    )
  }

  def main(args: Array[String]): Unit = {
    val fileCount = args(0).toInt
    val targetPath = args(1)
    val namePrefix = args(2)
//        val fileCount = 1
//        val targetPath = "D:\\testfile"
//        val namePrefix = "a"
    batchGenerator(fileCount, targetPath, namePrefix)
    log.info(s"finished! namePrefix:${namePrefix}")
  }
}
