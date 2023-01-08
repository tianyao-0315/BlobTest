package seaweedfs

import com.alibaba.fastjson2.JSON
import com.google.common.io.Files
import seaweedfs.client.{FilerClient, SeaweedOutputStream}

import java.io.{File, FileOutputStream}
import java.util
import java.util.concurrent.locks.ReentrantLock
import scala.util.Random

/**
 * @desc: TODO
 * @author: along
 * @date: 2022/11/29
 * @version: 1.0
 */
class SwUploadThread(n: Long,host:String, fileNames: Array[String], locks: Array[ReentrantLock],
                     countSize: Array[Long], countNum: Array[Long], filenameList: Array[File]) extends Runnable {
  override def run(): Unit = {
    val suffix = new Random().nextInt(locks.length)
    locks(suffix).lock()

    val fout = new FileOutputStream(filenameList(suffix), true)
    var j = 0L
    while (j < n / locks.length / 100) {
      fileNames.foreach(fileName => {
        val randFilename = System.currentTimeMillis() + fileName.split("/").last
        val fid = writeSingleFile(host,fileName,randFilename)
        countSize.update(suffix, countSize(suffix) + new File(fileName).length())
        countNum.update(suffix, countNum(suffix) + 1)
        fout.write((s"$fid\n").getBytes())
      })
      j += 1
    }
    fout.close()
    locks(suffix).unlock()
  }

  def writeSingleFile(host:String, localFilePath: String, remoteFileName: String): String = {
    val getResult = JSON.parseObject(HttpURLConnectionHelper.sendRequest(s"http://$host:9333/dir/assign","GET"))
    val files = new util.HashMap[String,String]()
    files.put(remoteFileName,localFilePath)
    val uploadUrl = getResult.get("publicUrl").toString
    val fid = getResult.get("fid").toString
    JSON.parseObject(HttpURLConnectionHelper.sendPost(
      s"http://${uploadUrl}/${fid}",null,null,files,"UTF-8","UTF-8"))
    fid
  }
}
