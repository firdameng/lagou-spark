package org.apache.spark.deploy

import org.apache.spark.SparkConf
import org.apache.spark.SecurityManager
import org.apache.spark.rpc.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
class MyWorker(
                val rpcEnv: RpcEnv,
                    cores: Int,
                    memory: Int,
                    masterRpcAddresses: Array[RpcAddress],
                    endpointName: String,
                    val conf: SparkConf,
                    val securityMgr: SecurityManager)
  extends ThreadSafeRpcEndpoint  {

  println("Mywork 主构造器运行")

  override def onStart(): Unit = {
    println("myworker  onStart方法运行,向mymaster注册.....")
    masterRpcAddresses.map { masterAddress =>
      println("Connecting to mymaster " + masterAddress + "...")
      val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
          masterEndpoint.send(TestAdd(100,200))
        }
      }



  override def receive: PartialFunction[Any, Unit] = {
    case TestAdd(x, y) => println(s"x=$x, y=$y; x+y=${x + y}")
    case _ => println("myworker 接收到未知消息")
  }
}
object Worker {
  val SYSTEM_NAME = "MyWorker"
  val ENDPOINT_NAME = "Worker"

  def main(argStrings: Array[String]) {


    val host = "localhost"
    val port = 6666
    val cores = 4
    val memory = 100000
    val masters:Array[String] = Array("spark://localhost:9999")

    val conf = new SparkConf
    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME
    val securityMgr = new SecurityManager(conf)

    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    val masterAddresses:Array[RpcAddress] = masters.map(RpcAddress.fromSparkURL(_))
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new MyWorker(rpcEnv, cores, memory,
      masterAddresses, ENDPOINT_NAME, conf, securityMgr))
    // 必须得有下面这一行
    rpcEnv.awaitTermination()
  }
}
