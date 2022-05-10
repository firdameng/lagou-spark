package org.apache.spark.deploy

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master.Master.{log, startRpcEnvAndEndpoint}
import org.apache.spark.deploy.master.MasterArguments
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.{SparkUncaughtExceptionHandler, Utils}
import org.apache.spark.SecurityManager

class MyMaster(
                override val rpcEnv: RpcEnv,
                address: RpcAddress,
                val securityMgr: SecurityManager,
                val conf: SparkConf)
  extends ThreadSafeRpcEndpoint {

  println("主构造器运行....")

  override def onStart(): Unit = {
    println("mymaster  onStart方法运行.....")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case TestAdd(x, y) => println(s"x=$x, y=$y; x+y=${x + y}")
    case _ => println("mymaster 接收到未知消息")
  }
}

object Master extends Logging {
  val SYSTEM_NAME = "MyMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    val conf = new SparkConf

    val host = "localhost"
    val port = 9999

    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new MyMaster(rpcEnv, rpcEnv.address, securityMgr, conf))

    masterEndpoint.send(TestAdd(10,20))

    rpcEnv.awaitTermination()
  }
}

case class TestAdd(x: Int, y: Int)
