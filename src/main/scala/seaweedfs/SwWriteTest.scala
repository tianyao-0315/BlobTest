package seaweedfs

import com.google.common.io.Files
import org.apache.log4j.Logger
import seaweedfs.client.{FilerClient, SeaweedInputStream, SeaweedOutputStream}

import java.io.{File, FileInputStream, FileOutputStream, IOException, InputStream}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ArrayBlockingQueue, Executors, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import scala.util.Random


/**
 * @desc: TODO
 * @author: along
 * @date: 2022/11/21
 * @version: 1.0
 */
object SwWriteTest {
  val logger = Logger.getLogger("Seaweedfs Test")
  val MAX_POOL_SIZE = 50
  val KEEP_ALIVE_TIME = 1L
  val QUEUE_CAPACITY = 200

  def main(args: Array[String]): Unit = {

    val host = args(0)
    val path = args(1)
    val list: String = args(2)
    val n: Long = args(3).toLong
    val CORE_POOL_SIZE = args(4).toInt

    val fileNames: Array[String] = new File(path).listFiles().map(file => file.getAbsolutePath)
    val countSize: Array[Long] = new Array[Long](CORE_POOL_SIZE)
    val countNum: Array[Long] = new Array[Long](CORE_POOL_SIZE)
    val filenameList: Array[File] = new Array[File](CORE_POOL_SIZE)

    /*创建存储文件id的目录*/
    val dir = new File(s"$list/test1yi")
    if(!dir.exists()){
      dir.mkdir()
    }

    /*每一个bucket有一把锁，防止线程争用同一个bucket导致抛出异常*/
    val locks: Array[ReentrantLock] = new Array[ReentrantLock](CORE_POOL_SIZE)
    for (i <- locks.indices) {
      locks(i) = new ReentrantLock()
      countSize(i) = 0
      countNum(i) = 0
      filenameList(i) = new File(s"$list/test1yi/file${i}_list.txt")
      if (!filenameList(i).exists()) {
        filenameList(i).createNewFile()
      }
    }

    val service = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
      new ArrayBlockingQueue[Runnable](QUEUE_CAPACITY),
      new ThreadPoolExecutor.DiscardPolicy()
    )

    var flag = false
    new Thread(() => {
      val t1 = System.currentTimeMillis()
      while (!flag) {
        val elapse = System.currentTimeMillis() - t1
        if (elapse % 5000 == 0) {
          println(s"transfer:${countSize.mkString(" ")}")
          println(s"obj numbers: ${countNum.mkString(" ")}")
          println(f"finish ${countNum.sum * 1.0 / n * 100}%.2f%% cost ${System.currentTimeMillis() - t1} ms")
        }
        flag = countNum.sum >= n
      }
      println(s"transfer:${countSize.mkString(" ")}")
      println(s"obj numbers: ${countNum.mkString(" ")}")
      println(f"finish ${countNum.sum * 1.0 / n * 100}%.2f%% cost ${System.currentTimeMillis() - t1} ms")
    }).start()

    while (!flag) {
      val work = new SwUploadThread(n,host,fileNames, locks, countSize, countNum, filenameList)
      service.execute(work)
    }

    service.shutdown()
    while (!service.isTerminated) {

    }

  }
}
