package seaweedfs

import com.alibaba.fastjson2.JSONObject

import java.io.File
import java.util.Scanner
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer

/**
 * @desc: TODO
 * @author: along
 * @date: 2022/11/30
 * @version: 1.0
 */
object SwReadTest {
  val MAX_POOL_SIZE = 50
  val KEEP_ALIVE_TIME = 1L
  val QUEUE_CAPACITY = 200

  def main(args: Array[String]): Unit = {

    val host = args(0)
    val list: String = args(1)
    val n: Long = args(2).toLong
    val CORE_POOL_SIZE = args(3).toInt

    val countNum: Array[Long] = new Array[Long](CORE_POOL_SIZE)

    /*创建存储文件id的目录*/
    val dir = new File(s"$list")
    val fidList = new ArrayBuffer[ArrayBuffer[String]](CORE_POOL_SIZE)
    dir.listFiles().foreach(file => {
      val in = new Scanner(file)
      var i = 0
      val fids = new ArrayBuffer[String]()
      var j = 0L
      while (j < (1000)) {
        fids.append(in.nextLine().trim)
        j += 1
      }
      if(i<CORE_POOL_SIZE)
        fidList.append(fids)
      i += 1
    })

    /*每一个bucket有一把锁，防止线程争用同一个bucket导致抛出异常*/
    val locks: Array[ReentrantLock] = new Array[ReentrantLock](CORE_POOL_SIZE)
    for (i <- locks.indices) {
      locks(i) = new ReentrantLock()
      countNum(i) = 0
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
          println(s"obj numbers: ${countNum.mkString(" ")}")
          println(f"finish ${countNum.sum * 1.0 / n * 100}%.2f%% cost ${System.currentTimeMillis() - t1} ms")
        }
        flag = countNum.sum >= n
      }
      println(s"obj numbers: ${countNum.mkString(" ")}")
      println(f"finish ${countNum.sum * 1.0 / n * 100}%.2f%% cost ${System.currentTimeMillis() - t1} ms")
    }).start()

    for(i<- 0 until(CORE_POOL_SIZE)) {
      val work = new SwReadThread(i,host, locks, fidList, countNum,(n/CORE_POOL_SIZE/1000).toInt)
      service.execute(work)
    }

    service.shutdown()
    while (!service.isTerminated) {

    }
  }
}
