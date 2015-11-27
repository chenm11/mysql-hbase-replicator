/**
  * Created by CM on 2015/11/4.
  *
  */
package mysql2hbase

import net.liftweb.json.Serialization.{read, write}
import net.liftweb.json._
import org.apache.spark.sql.hbase.util.{BinaryBytesUtils, BytesUtils, StringBytesUtils}
import org.apache.spark.sql.hbase.{KeyColumn, NonKeyColumn}
import org.apache.spark.sql.types._

object HbaseTableInfo {
  def apply(ser: String): HbaseTableInfo = {
    implicit val formats = Serialization.formats(NoTypeHints)
    read[HbaseTableInfo](ser)
  }

  def apply(dbName: String,
            dbTableName: String,
            hbaseTableName: String,
            hbaseNameSpace: String,
            sparkTableName: String,
            cols: Seq[HbaseCollInfo]
           ) = new HbaseTableInfo(dbName, dbTableName, hbaseTableName, hbaseNameSpace, sparkTableName, "Binary", cols)
}

case class HbaseTableInfo(dbName: String,
                          dbTableName: String,
                          hbaseTableName: String,
                          hbaseNameSpace: String,
                          sparkTableName: String,
                          bytesUtils: String,
                          cols: Seq[HbaseCollInfo]
                         ) {
  def getfullName() = dbName + "." + dbTableName

  def getBytesUtils(): BytesUtils = {
    bytesUtils match {
      case "String" => StringBytesUtils
      case _ => BinaryBytesUtils
    }
  }

  def getColumnFamilies() = {
    cols.groupBy(c => c.family).map(_._1)
  }

  def toJson() = {
    implicit val formats = Serialization.formats(NoTypeHints)
    write(this)
  }

  def toPrettyText() = {
    val sb = new StringBuilder
    val maxHeadLeftLength = Seq("dbName", "dbTableName","hbaseTableName",
      "hbaseNameSpace","sparkTableName","bytesUtils").map(_.length+2).max
    val maxHeadRightLength = Seq(dbName,  dbTableName,hbaseTableName,
       hbaseNameSpace,sparkTableName, bytesUtils).map(_.length+2).max
    sb.append("+-").append("-" * maxHeadLeftLength).append("-").
      append("-" * maxHeadRightLength).append("-+").append("\n")
    sb.append(" "*Integer2int(maxHeadLeftLength/2)+"%"+(+maxHeadLeftLength)+"s" format getfullName).append("\n")
    sb.append("+-").append("-" * maxHeadLeftLength).append("-").
      append("-" * maxHeadRightLength).append("-+").append("\n")
    sb.append("|").append(" %-"+maxHeadLeftLength+"s" format " dbName").append("|").
      append(" %-"+maxHeadRightLength+"s" format dbName).append("|").append("\n")
    sb.append("|").append(" %-"+maxHeadLeftLength+"s" format " dbTableName").append("|").
      append(" %-"+maxHeadRightLength+"s" format dbTableName).append("|").append("\n")
    sb.append("|").append(" %-"+maxHeadLeftLength+"s" format " hbaseTableName")
      .append("|").append(" %-"+maxHeadRightLength+"s" format hbaseTableName).append("|").append("\n")
    sb.append("|").append(" %-"+maxHeadLeftLength+"s" format " hbaseNameSpace").append("|")
      .append(" %-"+maxHeadRightLength+"s" format hbaseNameSpace).append("|").append("\n")
    sb.append("|").append(" %-"+maxHeadLeftLength+"s" format " sparkTableName").append("|").
      append(" %-"+maxHeadRightLength+"s" format sparkTableName).append("|").append("\n")
    sb.append("|").append(" %-"+maxHeadLeftLength+"s" format " bytesUtils").append("|").
      append(" %-"+maxHeadRightLength+"s" format bytesUtils).append("|").append("\n")
    sb.append("+-").append("-" * maxHeadLeftLength).append("|").
      append("-" * maxHeadRightLength).append("-+").append("\n")
    val lengthSet=(cols.map(_.lengthSeq()) ++ Seq(HbaseCollInfo.lengthSeq())).
      reduce((x, y) => {x.zip(y).map( v=> Math.max(v._1, v._2))})
    sb.append("+").append("-"*(lengthSet.reduce(_+_+2))).append("-+\n")
    sb.append(HbaseCollInfo.toPrettyText(lengthSet)).append("\n")
    sb.append("+").append("-"*(lengthSet.reduce(_+_+2))).append("-+\n")
    cols.map(c=>sb.append(c.toPrettyText(lengthSet)).append("\n"))
    sb.append("+").append("-"*(lengthSet.reduce(_+_+2))).append("-+\n")
    sb.toString()
  }

  def getKeyColumns(): Seq[KeyColumn] = {
    var ret = Seq[KeyColumn]()
    for (ordinal <- 0 until cols.length) {
      val col = cols(ordinal)
      if (col.isPrimaryKey) {
        val k = new KeyColumn(col.dbColumnName,
          HbaseCollInfo.getType(col.dbColumnType), ordinal)
        k.ordinal = ordinal
        ret = ret :+ k
      }
    }
    ret
  }

  def getNonKeyColumns(): Seq[NonKeyColumn] = {
    var ret = Seq[NonKeyColumn]()
    for (ordinal <- 0 until cols.length) {
      val col = cols(ordinal)
      if (!col.isPrimaryKey) {
        val k = new NonKeyColumn(col.dbColumnName,
          HbaseCollInfo.getType(col.dbColumnType), col.family, col.dbColumnName)
        k.ordinal = ordinal
        ret = ret :+ k
      }
    }
    ret
  }

  def sameData(other: HbaseTableInfo): Boolean = {
    this.getKeyColumns().sortBy(_.ordinal) == other.getKeyColumns().sortBy(_.ordinal) &&
      this.getNonKeyColumns().sortBy(_.sqlName) == other.getNonKeyColumns().sortBy(_.sqlName)
  }

  def sameData(other: TableInfo): Boolean = {
    val pks = this.getKeyColumns
    val otherPks = other.getKeyColumns
    val noPks = this.getNonKeyColumns
    val otherNoPks = other.getNonKeyColumns
    pks.map(i => otherPks.contains(i)).reduce(_ && _) &&
      otherPks.map(i => pks.contains(i)).reduce(_ && _) &&
      noPks.map(i => otherNoPks.contains(i)).reduce(_ && _) &&
      other.data.getDatabase.equalsIgnoreCase(dbName) &&
      other.data.getTable.equalsIgnoreCase(dbTableName)
  }
}


object HbaseCollInfo {
  def apply(dbColumnName: String, dbColumnType: String, isPrimaryKey: Boolean) = {
    new HbaseCollInfo(dbColumnName, dbColumnType,
      getDefaultNameMapping(dbColumnName), getDefaultTypeMapping(dbColumnType), isPrimaryKey)
  }

  def getDefaultNameMapping(colName: String) = {
    colName.replace("_", "")
  }

  def getType(orgType: String): DataType = {
    getDefaultTypeMapping(orgType) match {
      case "BooleanType" => DataTypes.BooleanType
      case "StringType" => DataTypes.StringType
      case "IntegerType" => DataTypes.IntegerType
      case "LongType" => DataTypes.LongType
      case "FloatType" => DataTypes.FloatType
      case "DoubleType" => DataTypes.DoubleType
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  def getDefaultTypeMapping(colType: String) = {
    implicit class Regex(sc: StringContext) {
      def r = new scala.util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
    }

    colType.toLowerCase match {
      case r"""bit(1)""" => "BooleanType"
      case r"""bit(\d+)""" => "StringType"
      case r"""tinyint""" => "IntegerType"
      case r"""bool""" => "BooleanType"
      case r"""boolean""" => "BooleanType"
      case r"""smallint.*unsigned""" => "IntegerType"
      case r"""smallint.*""" => "IntegerType"
      case r"""mediumint.*unsigned""" => "IntegerType"
      case r"""mediumint.*""" => "IntegerType"
      case r"""int.*""" => "IntegerType"
      case r"""integer.*unsigned""" => "LongType"
      case r"""integer.*""" => "IntegerType"
      case r"""bigint.*unsigned""" => "LongType"
      case r"""bigint.*""" => "LongType"
      case r"""float.*""" => "FloatType"
      case r"""double.*""" => "DoubleType"
      case _ => "StringType"
    }
  }
  def lengthSeq()={
    Seq(" dbColumnName ".length," dbColumnType ".length," hbaseColumnName ".length,
      " hbaseColumnType ".length," isPrimaryKey ".length," family ".length)
  }
  def toPrettyText(c:Seq[Int])={
    "+ %-"+c(0)+"s|"+" %-"+c(1)+"s|"+" %-"+c(2)+"s|"+" %-"+c(3)+"s|"+" %-"+c(4)+"s|"+" %-"+c(5)+"s+" format (
      "dbColumnName ","dbColumnType ","hbaseColumnName ","hbaseColumnType ","isPrimaryKey ","family ")

  }
}


case class HbaseCollInfo(dbColumnName: String,dbColumnType: String,hbaseColumnName: String,
                         hbaseColumnType: String,isPrimaryKey: Boolean,family: String = "cf"){
  def toPrettyText(c:Seq[Int])={
  "| %-"+c(0)+"s| "+"%-"+c(1)+"s| "+"%-"+c(2)+"s| "+"%-"+c(3)+"s| "+"%-"+c(4)+"s| "+
    "%-"+c(5)+"s| " format (dbColumnName,dbColumnType,hbaseColumnName,hbaseColumnType,isPrimaryKey.toString,family)
  }
  def lengthSeq()={
    Seq(dbColumnName.length,dbColumnType.length,hbaseColumnName.length,
      hbaseColumnType.length,isPrimaryKey.toString.length,family.length)
  }
}