package minio

import io.minio.MinioClient

import java.io.File
import java.util.Scanner
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.collection.mutable.ArrayBuffer

/**
 * @desc: TODO
 * @author: along
 * @date: 2022/11/30
 * @version: 1.0
 */
object MinioReadTest {
  val accessKey: String = "admin"
  val securityKey: String = "admin123"
  val MAX_POOL_SIZE = 300
  val QUEUE_CAPACITY = 10000
  val KEEP_ALIVE_TIME = 10L
  val BUCKET_NUM = 100

  def main(args: Array[String]): Unit = {

    val minioHostPath: String = args(0)
    val bucketName: String = args(1)
    val list: String = args(2)
    val n: Long = args(3).toLong
    val CORE_POOL_SIZE = args(4).toInt

    val countNum: Array[Long] = new Array[Long](CORE_POOL_SIZE)

    val dir = new File(s"$list/$bucketName")
    val fidList = new ArrayBuffer[ArrayBuffer[String]](CORE_POOL_SIZE)
    for(i<-0 until(CORE_POOL_SIZE)){
      fidList.append(new ArrayBuffer[String]())
    }

    dir.listFiles().foreach(file => {
      val in = new Scanner(file)
      var i = 0
      val fids = new ArrayBuffer[String]()
      var j = 0L
      while (j < (1000)) {
        fids.append(in.nextLine().trim)
        j += 1
      }
      if(i<CORE_POOL_SIZE) {
        val filename = file.getAbsolutePath.substring(file.getAbsolutePath.lastIndexOf("/")+1)
        fidList(filename(4)-'0') = fids
      }
      i += 1
    })

    val minioClient: MinioClient = MinioClient.builder().endpoint(minioHostPath, 9000, false).credentials(accessKey, securityKey).build()

    val executor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
      new ArrayBlockingQueue[Runnable](QUEUE_CAPACITY),
      new ThreadPoolExecutor.DiscardPolicy()
    )

    var flag = false

    new Thread(() => {
      val t1 = System.currentTimeMillis()
      while (!flag) {
        val elapse = System.currentTimeMillis() - t1
        if (elapse % 5000 == 0) {
          println(s"obj numbers: ${countNum.mkString(" ")}")
          println(f"finish ${countNum.sum * 1.0 / n * 100}%.2f%% cost ${System.currentTimeMillis() - t1} ms")
        }
        flag = countNum.sum >= n
      }
      println(s"obj numbers: ${countNum.mkString(" ")}")
      println(f"finish ${countNum.sum * 1.0 / n * 100}%.2f%% cost ${System.currentTimeMillis() - t1} ms")

    }).start()

    for(i<- 0 until CORE_POOL_SIZE) {
      val work = new MinioReadThread(i, minioClient, fidList, bucketName, countNum,(n/CORE_POOL_SIZE/1000).toInt)
      executor.execute(work)
    }

    executor.shutdown()
    while (!executor.isTerminated) {

    }
  }
}
