package seaweedfs

import com.alibaba.fastjson2.JSONObject

import java.io.{File, FileOutputStream}
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * @desc: TODO
 * @author: along
 * @date: 2022/11/30
 * @version: 1.0
 */
class SwReadThread(threadIdx:Int,host: String, locks: Array[ReentrantLock], fidList: ArrayBuffer[ArrayBuffer[String]], countNum: Array[Long],loop:Int) extends Runnable {
  override def run(): Unit = {
//    val suffix = new Random().nextInt(locks.length)
//    locks(suffix).lock()

    for(i<-0 until(loop)){
      fidList(threadIdx).foreach(fid => {
        readSingleFile(host, fid)
        countNum.update(threadIdx, countNum(threadIdx) + 1)
      })
    }
//    locks(suffix).unlock()
  }

  def readSingleFile(host: String, fid: String): Unit = {
    val vid = fid.substring(0, fid.indexOf(",")).toInt
    val firstGet = JSONObject.parseObject(HttpURLConnectionHelper.sendRequest(s"http://${host}:9333/dir/lookup?volumeId=${vid}", "GET"))
    val url = firstGet.getJSONArray("locations").getJSONObject(0).get("url").toString
    HttpURLConnectionHelper.sendRequest(s"http://${url}/$fid","GET")
  }
}
