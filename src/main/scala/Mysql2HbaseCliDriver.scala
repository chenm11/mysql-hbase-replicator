package mysql2hbase

import jline.ConsoleReader
import org.apache.spark.sql.hbase.HBase2Catalog

/**
  * Created by CM on 2015/11/9.
  */
object Mysql2HbaseCliDriver {
  private val prompt = "Mysql2Hbase"
  private val sep = "-".padTo(prompt.length * 8, ' ')

  def promptPrefix = s"$prompt"

  def main(args: Array[String]): Unit = {
    val reader = new ConsoleReader()
    reader.setBellEnabled(false)
    var line = reader.readLine(promptPrefix + "> ")
    while (line != null) {
      line = reader.readLine(promptPrefix + "> ")
      processLine(line.trim, allowInterrupting = true)
    }
    System.exit(0)
  }

  private def doExit() = {
    System.exit(0)
  }

  private def doHelp() = {
    println("==========delete=============")
    println("delete table defined in meta, usage:\n delete [tablename]\n show ")
    println("==========list=============")
    println("show table defined in meta, usage:\n list [tablename]\n list ")
    println("==========map=============")
    println("map table to hbase-sapark, usage:\n map tablename\n map ")
    println("==========add=============")
    println("add to define meta in json format, usage:\n add json")
    println("--------------- json example---------------")
    println(
      """{"dbName":"mixfs","dbTableName":"student","hbaseTableName":"mixfs:student2","hbaseNameSpace":"mixfs","sparkTableName":"student2","bytesUtils":"Binary","cols":[
        |    {"dbColumnName":"sn","dbColumnType":"int(11)","hbaseColumnName":"sn","hbaseColumnType":"IntegerType","isPrimaryKey":true,"family":"cf"},
        |    {"dbColumnName":"id","dbColumnType":"int(11)","hbaseColumnName":"id","hbaseColumnType":"IntegerType","isPrimaryKey":true,"family":"cf"},
        |    {"dbColumnName":"score","dbColumnType":"int(11)","hbaseColumnName":"score","hbaseColumnType":"IntegerType","isPrimaryKey":false,"family":"cf"},
        |    {"dbColumnName":"name","dbColumnType":"varchar(10)","hbaseColumnName":"name","hbaseColumnType":"StringType","isPrimaryKey":false,"family":"cf"},
        |    {"dbColumnName":"rem","dbColumnType":"varchar(2)","hbaseColumnName":"rem","hbaseColumnType":"StringType","isPrimaryKey":false,"family":"cf"}
        |    ]
        |}""".stripMargin)
    println("--------------- json example---------------")
  }

  private def doList(tableNames: Seq[String]) = {
    if(tableNames.length==0){
      HBaseTableUtils.getHbaseTableInfoMap().keySet.map(println)
    }
    for (tableName <- tableNames) {
        HBaseTableUtils.getRelation(tableName) match {
          case Some(table) => {
            println(table.toPrettyText())
          }
          case None =>
            Log.warn("No table named {} found", tableName)
        }
      }
  }

  private def doNothing(cmd: String) = {
    if(!cmd.matches("^\\s*$")) {
      println(cmd + " is not supported")
    }
  }

  private def doAdd(json: String) = {
    try {
      HBaseTableUtils.writeToTable(json)
    } catch {
      case e: Exception => Log.warn("json parse or hbase error")
    }
  }

  private def doMapping(tableNames: Seq[String]): Unit = {
    if(tableNames.length==0){
      println("map table to hbase-sapark, usage:\n map tablename\n map ")
    }
    for (tableName <- tableNames) {
      println("-----add mapping for -------" + tableName)
      HBaseTableUtils.getRelation(tableName)match {
        case Some(table) => HBase2Catalog.mappingTableToSpark(table)
        case None => Log.warn("No table named {} found", tableName)
      }
    }
  }

  def unDoMapping(tableNames: Seq[String]) = {
    for (tableName <- tableNames) {
      HBase2Catalog.deleteTableInSpark(tableName)
      println("-----del mapping for -------" + tableName)
    }
  }

  private def doDelete(tableNames: Seq[String]) = {
    for (tableName <- tableNames) {
      if (!tableName.matches("^\\s*$")) {
        HBaseTableUtils.getRelation(tableName) match {
          case Some(table) => {
            HBaseTableUtils.deleteTable(tableName)
            println(tableName + " deleted")
          }
          case None => Log.warn("No table named {} found", tableName)
        }
      }
    }
  }


  private def processLine(input: String, allowInterrupting: Boolean) = {
    val token = input.split("\\s+")
    token(0).toUpperCase match {
      case "EXIT" => doExit()
      case "QUIT" => doExit()
      case "HELP" => doHelp()
      case "LIST" => doList(token.tail)
      case "ADD" => doAdd(token.tail.mkString(""))
      case "DELETE" => doDelete(token.tail)
      case "MAP" => doMapping(token.tail)
      case "UNMAP" => unDoMapping(token.tail)
      case _ => doNothing(_)
    }
  }
}
