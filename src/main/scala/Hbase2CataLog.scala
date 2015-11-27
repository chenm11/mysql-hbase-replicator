package org.apache.spark.sql.hbase

import mysql2hbase.{HBaseTableUtils, HbaseTableInfo}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by CM on 2015/11/3.
 *  all the things todo with spark-hbase(astro)
 */

object HBase2Catalog {
  lazy val ctx = new HBaseSQLContext(
    new SparkContext("local[2]", "HbaseSQLContext", new SparkConf(true)))
  lazy val logger = Logger.getLogger(getClass.getName)
  lazy val catalog: HBaseCatalog =ctx.catalog

  def mappingTableToSpark(table:HbaseTableInfo)={
    catalog.createTable(table.sparkTableName,
      table.hbaseNameSpace,table.hbaseTableName,
      table.getNonKeyColumns()++table.getKeyColumns(),null)
  }

  def deleteTableInSpark(tableName:String)={
    catalog.deleteTable(HBaseTableUtils.getRelation(tableName).get.sparkTableName)
  }
}