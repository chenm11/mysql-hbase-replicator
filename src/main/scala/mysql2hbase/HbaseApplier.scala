package mysql2hbase

import java.io.Serializable
import java.lang.management.ManagementFactory
import java.util
import java.util.BitSet
import java.util.concurrent.atomic.AtomicLong

import com.github.shyiko.mysql.binlog.event.{DeleteRowsEventData, UpdateRowsEventData, WriteRowsEventData}
import org.apache.commons.lang3.time.StopWatch
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer


trait HbaseApplierMBean {
  def getCount(): util.HashMap[String, AtomicLong]

  def getDelay(): util.HashMap[String,  ArrayBuffer[Long]]

  def getBinlog(): String

  def getBinlogPosition(): Long

}


object HbaseApplier {
  def login(hbaseConf: Configuration) = {
    val krb = Config.getKrbLogin()
    UserGroupInformation.setConfiguration(hbaseConf)
    UserGroupInformation.loginUserFromKeytab(krb._1, krb._2)
  }


  def initCount = {
    val count = new util.HashMap[String, AtomicLong]()
    count.put("insert", new AtomicLong)
    count.put("delete", new AtomicLong)
    count.put("update.insert", new AtomicLong)
    count.put("update.delete", new AtomicLong)
    count
  }

  def initDelay = {
    val delay = new util.HashMap[String, ArrayBuffer[Long]]()
    delay.put("insert", ArrayBuffer[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    delay.put("delete", ArrayBuffer[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    delay.put("update.insert", ArrayBuffer[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    delay.put("update.delete", ArrayBuffer[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    delay
  }

}


class HbaseApplier(hbaseConfPath: Seq[String], hbaseBinlogTable: String, hbaseBinlogKey: String) extends HbaseApplierMBean {
  var count = HbaseApplier.initCount
  var delay = HbaseApplier.initDelay
  var binlog: String = ""
  var binlogPosition: Long = 0L

  override def getCount = count

  override def getDelay = delay

  override def getBinlog = binlog

  override def getBinlogPosition = binlogPosition

  val hbaseConf = HBaseConfiguration.create()
  hbaseConfPath.foreach { f => hbaseConf.addResource(new Path(f)) }
  HbaseApplier.login(hbaseConf)
  val hbaseAdmin = new HBaseAdmin(hbaseConf)
  val binlogTable = getTable(hbaseBinlogTable)


  def getTable(tableName: String) = {
    if (!hbaseAdmin.tableExists(tableName)) {
      val htd = new HTableDescriptor(TableName.valueOf(tableName))
      htd.addFamily(new HColumnDescriptor("cf"))
      htd.addFamily(new HColumnDescriptor("pk"))
      hbaseAdmin.createTable(htd)
    }
    new HTable(hbaseConf, Bytes.toBytes(tableName))
  }

  def binlogGetPosition: Option[(String, Long)] = {
    val get = new Get(Bytes.toBytes(hbaseBinlogKey))
    val result = binlogTable.get(get)
    if (!result.isEmpty) {
      val fn = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("filename"))
      val pst = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("position"))
      binlog = Bytes.toString(fn)
      binlogPosition = Bytes.toLong(pst)
      Log.info("read binglog {} {}", Bytes.toString(fn), Bytes.toLong(pst))
      Option(Bytes.toString(fn), Bytes.toLong(pst))
    } else {
      None
    }
  }

  def binlogRotate(filename: String, position: Long) {
    //change to another filename position
    val put = new Put(Bytes.toBytes(hbaseBinlogKey))
    put.add(Bytes.toBytes("cf"), Bytes.toBytes("filename"), Bytes.toBytes(filename))
    put.add(Bytes.toBytes("cf"), Bytes.toBytes("position"), Bytes.toBytes(position))
    binlogTable.put(put)
    binlog = filename
    binlogPosition = position
    Log.info("rotate to binglog {} {}", filename, position)
  }

  def binlogNextPosition(position: Long) {
    //position =  new position
    val put = new Put(Bytes.toBytes(hbaseBinlogKey))
    put.add(Bytes.toBytes("cf"), Bytes.toBytes("position"), Bytes.toBytes(position))
    binlogTable.put(put)
    binlogPosition = position
    Log.info("next binglog position {}", position)
  }

  def toScalaBitSet(s: BitSet): scala.collection.mutable.BitSet = {
    new scala.collection.mutable.BitSet(s.toLongArray)
  }


  def getRowKey(includedColumns: BitSet, tableInfo: TableInfo, row: Array[Serializable]): Array[Byte] = {
    val cols = tableInfo.cols
    val pk = toScalaBitSet(tableInfo.primaryKey)
    val included = toScalaBitSet(includedColumns)
    if ((pk & included) == pk) {
      //primary key must complete
      var buffer = new ArrayBuffer[Serializable]
      for (i <- 0 until cols.size) {
        if (includedColumns.get(i) && tableInfo.primaryKey.get(i)) {
          buffer.append(row(i))
        }
      }
      SerializableUtil.serializableToBytes(buffer)
    } else {
      Array[Byte](0)
    }
  }

  def getDelete(rowKey: Array[Byte]): Delete = {
    new Delete(rowKey)
  }

  def getPut(rowKey: Array[Byte], includedColumns: java.util.BitSet, tableInfo: TableInfo, row: Array[Serializable]): Put = {
    val put = new Put(rowKey)
    val cols = tableInfo.cols
    for (i <- 0 until cols.size) {
      if (includedColumns.get(i)) {
        if (tableInfo.primaryKey.get(i)) {
          put.add(Bytes.toBytes("pk"), Bytes.toBytes(cols(i).name), SerializableUtil.serializableToBytes(row(i)))
        } else {
          put.add(Bytes.toBytes("cf"), Bytes.toBytes(cols(i).name), SerializableUtil.serializableToBytes(row(i)))
        }
      }
    }
    put
  }

  def timedHbaseDataAction[A](actionType: String)(action: => Unit) = {
    val sw = new StopWatch
    sw.reset()
    sw.start()
    action
    sw.stop()
    val d = delay.get(actionType)
    d.remove(0)
    d.append(sw.getTime)
    count.get(actionType).incrementAndGet()
  }


  def insert(nextPosition: Long, tableInfo: TableInfo, data: WriteRowsEventData) {
    val table = getTable(tableInfo.getHTableName())
    for (mySQLValues <- data.getRows.asScala) {
      val rowKey = getRowKey(data.getIncludedColumns, tableInfo, mySQLValues)
      if (rowKey.length == 0) {
        Log.error("row key is null {}")
      }
      val put = getPut(rowKey, data.getIncludedColumns, tableInfo, mySQLValues)
      timedHbaseDataAction("insert")(table.put(put))
    }
    Log.info("insert {} log position {}", data.toString, nextPosition)
    binlogNextPosition(nextPosition)
  }

  def isSameRowKey(left: Array[Byte], right: Array[Byte]): Boolean = {
    left.toList == right.toList
  }

  def update(nextPosition: Long, tableInfo: TableInfo, data: UpdateRowsEventData) {
    val table = getTable(tableInfo.getHTableName())
    val watch = new StopWatch()
    for (entry <- data.getRows.asScala) {
      val rowKeyBefore = getRowKey(data.getIncludedColumnsBeforeUpdate, tableInfo, entry.getKey)
      val rowKeyAfter = getRowKey(data.getIncludedColumns, tableInfo, entry.getValue)
      Log.info("match row key is {}", isSameRowKey(rowKeyBefore, rowKeyAfter))
      if (isSameRowKey(rowKeyBefore, rowKeyAfter)) {
        val del = getDelete(rowKeyBefore)
        timedHbaseDataAction("update.delete")(table.delete(del))
      }
      val put = getPut(rowKeyAfter, data.getIncludedColumns, tableInfo, entry.getValue)
      timedHbaseDataAction("update.insert")(table.put(put))
    }
    Log.info("update {} log position {}", data.toString, nextPosition)
    binlogNextPosition(nextPosition)
  }

  def remove(nextPosition: Long, tableInfo: TableInfo, data: DeleteRowsEventData) {
    val table = getTable(tableInfo.getHTableName())
    val watch = new StopWatch()
    for (mySQLValues <- data.getRows.asScala) {
      val rowKey = getRowKey(data.getIncludedColumns, tableInfo, mySQLValues)
      val del = getDelete(rowKey)
      timedHbaseDataAction("delete")(table.delete(del))
    }
    Log.info("remove {} log position {}", data.toString, nextPosition)
    binlogNextPosition(nextPosition)
  }
}