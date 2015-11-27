package mysql2hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable

/**
  * Created by CM on 2015/11/6.
  * all the things todo with self defined relations
  * mysql-hbase-spark
  */
object HBaseTableUtils {
  private final val META_DATA = "mysql2hbase"
  private final val META_COLUMN_FAMILY = Bytes.toBytes("cf")
  private final val META_COLUMN = Bytes.toBytes("data")
  var relationMap = getHbaseTableInfoMap

  def getMetadataTable: HTable = {
    HbaseApplier.getTable(META_DATA) match{
      case None => throw new Exception("meta table: "+META_DATA+" is not exist")
      case Some(meta)=>meta
    }
  }

  def isTableNeedReplicated(dbName:String,tableName:String):Boolean={
    val fullName = tableName.toLowerCase
    relationMap.keySet.contains(fullName)
  }

  def deleteTable(tableName: String)={
    if(isTableExists(tableName)){
      val metadataTable = getMetadataTable
      val del = new Delete(Bytes.toBytes(tableName))
      metadataTable.delete(del)
      relationMap.remove(tableName)
      metadataTable.flushCommits()
      metadataTable.close()
    }else{
      Log.warn("table {} not exists, stop delete",tableName)
    }
  }

   def writeToTable(jsonStr: String):Unit = {
     val hbaseTableInfo=HbaseTableInfo(jsonStr)
     writeToTable(hbaseTableInfo)
   }

  def isTableExists(tableName: String): Boolean ={
    relationMap = getHbaseTableInfoMap
    relationMap.keySet.contains(tableName)
  }

  private def writeToTable(hbaseTableInfo: HbaseTableInfo) = {
    val metadataTable = getMetadataTable
    val tableName = hbaseTableInfo.getfullName
    if(isTableExists(tableName)){
      Log.warn("table {} already exists, overwrite and continue",tableName)
    }
    val put = new Put(Bytes.toBytes(tableName))
    put.add(META_COLUMN_FAMILY, META_COLUMN, Bytes.toBytes(hbaseTableInfo.toJson()))
    metadataTable.put(put)
    metadataTable.flushCommits()
    metadataTable.close()
  }


  private def getHbaseTableInfoFromResult(result: Result): HbaseTableInfo = {
    val value = result.getValue(META_COLUMN_FAMILY, META_COLUMN)
    val ret = Bytes.toString(value)
    HbaseTableInfo(ret)
  }

  def getRelation(name: String):Option[HbaseTableInfo] = {
    if(relationMap.size==0){
      relationMap=getHbaseTableInfoMap
    }
    relationMap.get(name)
  }


  def getHbaseTableInfoMap() = {
    val relations = new mutable.HashMap[String, HbaseTableInfo]
      with mutable.SynchronizedMap[String, HbaseTableInfo]
    val scan = new Scan()
    scan.addColumn(META_COLUMN_FAMILY, META_COLUMN)
    val scanner = getMetadataTable.getScanner(scan)
    import scala.collection.JavaConversions.asScalaIterator//change to scala iterator
    for ( result <- scanner.iterator) {
      val rel = getHbaseTableInfoFromResult(result)
      relations += (rel.getfullName -> rel)
    }
    relations
  }
}
