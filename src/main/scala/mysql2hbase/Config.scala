package mysql2hbase


import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

case class Config(
                   myHost: String, myPort: Int, myServerId: Long,
                   myUsername: String, myPassword: String,
                   //myDatabasesOnly: Seq[String],
                   hbaseConfPath:  Seq[String],
                   //hbaseBinlogTable: String,
                   hbaseBinlogKey: String,
                   //hbaseMapTable:String,
                   maxFailedEventQueueSize: Int
                   )


object Config {
  def getKrbLogin()={
    val conf = ConfigFactory.load()
    val mydit = conf.getConfig("mysql2hbase")
    (mydit.getString("krb.principle"),mydit.getString("krb.keytab"))
  }
  def load(): Config = {
    val conf = ConfigFactory.load()
    val mydit = conf.getConfig("mysql2hbase")
    val myHost = mydit.getString("mysql.host")
    val myPort = mydit.getInt("mysql.port")
    val myServerId = mydit.getLong("mysql.serverid")
    val myUsername = mydit.getString("mysql.username")
    val myPassword = mydit.getString("mysql.password")
    val hbaseConfPath = mydit.getStringList("hbase.conf").asScala
    val hbaseBinlogKey = mydit.getString("hbase.binlogKey")
    val maxFailedEventQueueSize = mydit.getInt("maxFailedEventQueueSize")

    Config(
      myHost, myPort, myServerId, myUsername, myPassword,// myDatabasesOnly,
      hbaseConfPath,hbaseBinlogKey, //hbaseBinlogTable, hbaseMapTable,
      maxFailedEventQueueSize
    )
  }
}
