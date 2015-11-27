import org.slf4j.LoggerFactory

package object mysql2hbase {
  val Log = LoggerFactory.getLogger(getClass.getPackage.getName)
}
package object hbase {
  type HBaseRawType = Array[Byte]
}