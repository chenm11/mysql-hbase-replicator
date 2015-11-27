package mysql2hbase

import java.io.Serializable
import java.util
import java.util.BitSet
import java.util.concurrent.atomic.AtomicLong
import com.github.shyiko.mysql.binlog.event.{DeleteRowsEventData, UpdateRowsEventData, WriteRowsEventData}
import org.apache.commons.lang3.time.StopWatch
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.Row
import org.apache.spark.sql.hbase.util.{DataTypeUtils, HBaseKVHelper}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


trait HbaseApplierMBean {
  def getCount(): util.HashMap[String, AtomicLong]

  def getDelay(): util.HashMap[String, ArrayBuffer[Long]]

  def getBinlog(): String

  def getBinlogPosition(): Long

  final val INSERT="insert"
  final val DELETE="delete"
  final val UPDATE_INSERT="update.insert"
  final val UPDATE_DELETE="update.delete"

  def initCount = {
    val count = new util.HashMap[String, AtomicLong]()
    count.put(INSERT, new AtomicLong)
    count.put(DELETE, new AtomicLong)
    count.put(UPDATE_INSERT, new AtomicLong)
    count.put(UPDATE_DELETE, new AtomicLong)
    count
  }

  def initDelay = {
    val delay = new util.HashMap[String, ArrayBuffer[Long]]()
    delay.put(INSERT, ArrayBuffer[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    delay.put(DELETE, ArrayBuffer[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    delay.put(UPDATE_INSERT, ArrayBuffer[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    delay.put(UPDATE_DELETE, ArrayBuffer[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    delay
  }
}


object HbaseApplier {
  final val BINLOG_TABLE = "BinlogTable"
  final val BINLOG__COLUMN_FAMILY = "cf"
  final val BINLOG__FILENAME_COLUMN = "filename"
  final val BINLOG__POSITION_COLUMN = "position"
  val hbaseConf = HBaseConfiguration.create()
  var hbaseAdmin = new HBaseAdmin(hbaseConf)
  lazy val tables = collection.mutable.Map[String, HTable]()

  HbaseApplier.login(hbaseConf)
  def addConfPath(hbaseConfPath: Seq[String]) = {
    hbaseConfPath.foreach { f => hbaseConf.addResource(new Path(f)) }
    hbaseAdmin = new HBaseAdmin(hbaseConf)
  }

  def getTableForDatabase(dbTableName:String)={
    HBaseTableUtils.getRelation(dbTableName) match {
      case None =>
        throw new Exception("meta table mapping not defined: " + dbTableName)
      case Some(relation) => {
        HbaseApplier.getTable(relation.hbaseTableName) match{
          case None =>
            throw new Exception("hbase data table not defined: " + dbTableName)
          case Some(dataTable) => dataTable
        }
      }
    }
  }

  def getTable(tableName: String):Option[HTable] = {
    //not to create table automaticlly
    tables.get(tableName) match{
      case Some(table) => Some(table)
      case None=>{
        if (hbaseAdmin.tableExists(tableName)) {
          None
        }
        val table =new HTable(hbaseConf, Bytes.toBytes(tableName))
        table.setAutoFlushTo(false)
        tables.put(tableName,table)
        Some(table)
      }
    }
  }

  def login(hbaseConf: Configuration) = {
    val krb = Config.getKrbLogin()
    UserGroupInformation.setConfiguration(hbaseConf)
    UserGroupInformation.loginUserFromKeytab(krb._1, krb._2)
  }
}


class HbaseApplier(hbaseConfPath: Seq[String], hbaseBinlogKey: String) extends HbaseApplierMBean {
  var count = initCount
  var delay = initDelay
  var binlog: String = ""
  var binlogPosition: Long = 0L
  HbaseApplier.addConfPath(hbaseConfPath)
  val binlogTable = HbaseApplier.getTable(HbaseApplier.BINLOG_TABLE) match{
    case None =>
      throw new Exception("hbase meta table not defined: " + HbaseApplier.BINLOG_TABLE)
    case Some(binlogTable) => binlogTable
  }
  override def getCount = count
  override def getDelay = delay
  override def getBinlog = binlog
  override def getBinlogPosition = binlogPosition

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


  def binlogGetPosition: Option[(String, Long)] = {
    val get = new Get(Bytes.toBytes(hbaseBinlogKey))
    val result = binlogTable.get(get)
    if (!result.isEmpty) {
      val fn = result.getValue(Bytes.toBytes(HbaseApplier.BINLOG__COLUMN_FAMILY),
        Bytes.toBytes(HbaseApplier.BINLOG__FILENAME_COLUMN))
      val pst = result.getValue(Bytes.toBytes(HbaseApplier.BINLOG__COLUMN_FAMILY),
        Bytes.toBytes(HbaseApplier.BINLOG__POSITION_COLUMN))
      binlog = Bytes.toString(fn)
      binlogPosition = Bytes.toLong(pst)
      Log.info("read binglog {} {}", Bytes.toString(fn), Bytes.toLong(pst))
      Option(Bytes.toString(fn), Bytes.toLong(pst))
    } else {
      None
    }
  }

  def binlogRotate(filename: String, position: Long) {
    val put = new Put(Bytes.toBytes(hbaseBinlogKey))
    put.add(Bytes.toBytes(HbaseApplier.BINLOG__COLUMN_FAMILY),
      Bytes.toBytes(HbaseApplier.BINLOG__FILENAME_COLUMN), Bytes.toBytes(filename))
    put.add(Bytes.toBytes(HbaseApplier.BINLOG__COLUMN_FAMILY),
      Bytes.toBytes(HbaseApplier.BINLOG__POSITION_COLUMN), Bytes.toBytes(position))
    binlogTable.put(put)
    binlog = filename
    binlogPosition = position
    binlogTable.flushCommits()
    Log.info("rotate to binglog {} {}", filename, position)
  }

  def binlogNextPosition(position: Long) {
    val put = new Put(Bytes.toBytes(hbaseBinlogKey))
    put.add(Bytes.toBytes(HbaseApplier.BINLOG__COLUMN_FAMILY),
      Bytes.toBytes(HbaseApplier.BINLOG__POSITION_COLUMN), Bytes.toBytes(position))
    binlogTable.put(put)
    binlogPosition = position
    binlogTable.flushCommits()
    Log.info("next binglog position {}", position)
  }

  def toScalaBitSet(s: BitSet): scala.collection.mutable.BitSet = {
    new scala.collection.mutable.BitSet(s.toLongArray)
  }

  def getPutForSpark(rowKey: Array[Byte], includedColumns: java.util.BitSet,
                     tableInfo: TableInfo, row: Array[Serializable]): Put = {
    val put = new Put(rowKey)
    val relation = HBaseTableUtils.getRelation(tableInfo.getDBTableName()).get
    val theSparkRow = Row.fromSeq(row)
    relation.getNonKeyColumns().foreach(
      nkc => {
        val rowVal = DataTypeUtils.getRowColumnInHBaseRawType(
          theSparkRow, nkc.ordinal, nkc.dataType, relation.getBytesUtils())
        put.add(nkc.familyRaw, nkc.qualifierRaw, rowVal)
      }
    )
    put
  }

  def getRowKeyForSpark(includedColumns: BitSet,
                        tableInfo: TableInfo,
                        row: Array[Serializable]): Array[Byte] = {
    val cols = tableInfo.cols
    val pk = toScalaBitSet(tableInfo.primaryKey)
    val included = toScalaBitSet(includedColumns)
    HBaseTableUtils.getRelation(tableInfo.getDBTableName()) match {
      case None => throw new Exception("can't get table for " + tableInfo.getDBTableName())
      case Some(relation) => {
        if ((pk & included) != pk) {
          throw new Exception("sql statement does not contain all primary keys")
        }
        val theSparkRow = Row.fromSeq(row)
        val rawKeyCol = relation.getKeyColumns().map(
          kc => {
            val rowColumn = DataTypeUtils.getRowColumnInHBaseRawType(
              theSparkRow, kc.ordinal, kc.dataType)
            (rowColumn, kc.dataType)
          }
        )
        HBaseKVHelper.encodingRawKeyColumns(rawKeyCol)
      }
    }
  }

  def getDelete(rowKey: Array[Byte]): Delete = {
    new Delete(rowKey)
  }

  def insert(nextPosition: Long, tableInfo: TableInfo, data: WriteRowsEventData) {
    val table =
      HbaseApplier.getTableForDatabase(tableInfo.getDBTableName())
    for (mySQLValues <- data.getRows.asScala) {
      val rowKey = getRowKeyForSpark(data.getIncludedColumns, tableInfo, mySQLValues)
      if (rowKey.length == 0) {
        Log.error("row key is null {}")
      }
      val put = getPutForSpark(rowKey, data.getIncludedColumns, tableInfo, mySQLValues)
      timedHbaseDataAction(INSERT)(table.put(put))
    }
    table.flushCommits()
    Log.info("insert to  values  {} log position {}", data.toString, nextPosition)
    binlogNextPosition(nextPosition)
  }

  def isSameRowKey(left: Array[Byte], right: Array[Byte]): Boolean = {
    left.toList == right.toList
  }

  def update(nextPosition: Long, tableInfo: TableInfo, data: UpdateRowsEventData) {
    val table =
      HbaseApplier.getTableForDatabase(tableInfo.getDBTableName())
    for (entry <- data.getRows.asScala) {
      val rowKeyBefore = getRowKeyForSpark(data.getIncludedColumnsBeforeUpdate, tableInfo, entry.getKey)
      val rowKeyAfter = getRowKeyForSpark(data.getIncludedColumns, tableInfo, entry.getValue)
      if (isSameRowKey(rowKeyBefore, rowKeyAfter)) {
        val del = getDelete(rowKeyBefore)
        timedHbaseDataAction(UPDATE_DELETE)(table.delete(del))
      }
      val put = getPutForSpark(rowKeyAfter, data.getIncludedColumns, tableInfo, entry.getValue)
      timedHbaseDataAction(UPDATE_INSERT)(table.put(put))
    }
    table.flushCommits()
    Log.info("update {} log position {}", data.toString, nextPosition)
    binlogNextPosition(nextPosition)
  }

  def remove(nextPosition: Long, tableInfo: TableInfo, data: DeleteRowsEventData) {
    val table =
      HbaseApplier.getTableForDatabase(tableInfo.getDBTableName())
    for (mySQLValues <- data.getRows.asScala) {
      val rowKey = getRowKeyForSpark(data.getIncludedColumns, tableInfo, mySQLValues)
      val del = getDelete(rowKey)
      timedHbaseDataAction(DELETE)(table.delete(del))
    }
    table.flushCommits()
    Log.info("remove {} log position {}", data.toString, nextPosition)
    binlogNextPosition(nextPosition)
  }
}