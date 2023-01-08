package minio

import io.minio.{BucketExistsArgs, GetObjectArgs, MakeBucketArgs, MinioClient, UploadObjectArgs}

import java.io.{File, FileOutputStream, InputStream}
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * @desc: TODO
 * @author: along
 * @date: 2022/11/30
 * @version: 1.0
 */
class MinioReadThread(threadIdx:Int, minioClient: MinioClient, fileNames: ArrayBuffer[ArrayBuffer[String]], bucketName: String,
                      countNum: Array[Long],loop:Int) extends Runnable{
  override def run(): Unit = {
    for(i<-0 until(loop)){
      fileNames(threadIdx).foreach(filename => {
        get(minioClient,filename, bucketName+threadIdx)
        countNum.update(threadIdx, countNum(threadIdx) + 1)
      })
    }
  }

  def get(minioClient: MinioClient, objectName: String, bucketName: String): Unit = {
    val stream: InputStream = minioClient.getObject(
      GetObjectArgs.builder()
        .bucket(bucketName)
        .`object`(objectName)
        .build())
    stream.read(new Array[Byte](1024 * 1024))
    stream.close()
  }
}
