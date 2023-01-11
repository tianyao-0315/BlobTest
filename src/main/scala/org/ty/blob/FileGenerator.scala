package org.ty.blob

import org.apache.log4j.Logger

import java.io.{BufferedOutputStream, File, PrintWriter}
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Random

object FileGenerator {

  val log = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val FILE_NUMS = args(0).toInt
    val FILE_SIZE = args(1).toInt
    val STR_LENGTH = args(2).toInt
    val FILE_DIR = args(3)
//        val FILE_NUMS = 1
//        val FILE_SIZE = 200
//        val STR_LENGTH = 700
//        val FILE_DIR = "D:\\testfile\\1w-400"

    implicit lazy val ec: ExecutionContextExecutorService = {
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(20))
    }

    val FILE_IDENTITY = FILE_DIR.substring(FILE_DIR.indexOf("-") + 1)

    val rand = new Random()
    val st = System.currentTimeMillis()
    val taskArr = new ArrayBuffer[Future[Boolean]]()
    log.info(s"starting generate ${FILE_NUMS} files... ")
    for (i <- 0 until (FILE_NUMS)) {
      val task = Future {
        val dir = new File(FILE_DIR)
        if (!dir.exists())
          dir.mkdir()
        val file = new File("%s/test_file_%s_%06d.txt".format(FILE_DIR, FILE_IDENTITY, i))
        val writer = new PrintWriter(file)
        for (j <- 0 to FILE_SIZE) {
          writer.write(rand.nextString(STR_LENGTH) + "\n")
        }
        writer.close()
        log.info("wrote file %s/test_file_%s_%02d.txt".format(FILE_DIR, FILE_IDENTITY, i))
        true
      }
      taskArr.append(task)
    }
    taskArr.foreach(Await.result(_, Duration.Inf))
    ec.shutdown()
    log.info(s"finished! cost ${System.currentTimeMillis() - st}ms")
  }
}
