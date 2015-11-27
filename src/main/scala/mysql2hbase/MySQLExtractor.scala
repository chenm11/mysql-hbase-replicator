package mysql2hbase


import java.util.concurrent.atomic.AtomicLong
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.BinaryLogClient.{EventListener, LifecycleListener}
import com.github.shyiko.mysql.binlog.event.{DeleteRowsEventData, Event, EventData, EventHeaderV4, FormatDescriptionEventData, RotateEventData, TableMapEventData, UpdateRowsEventData, WriteRowsEventData}
import org.apache.log4j.Logger
import scala.util.control.NonFatal
object MySQLExtractor {
  private final val RECONNECT_DELAY_SECS = 5
}

/**
  * Error handling:
  *
  * When there's problem, it will be logged.
  *
  * If MySQLExtractor can't connect to MySQL, it will automatically retry every
  * 5 seconds.
  *
  * If MySQLExtractor can't connect to MySQL because the specified binlog file
  * name and position is too old, the replicator process will exit.
  *
  *
  */
trait MySQLExtractorMBean {
  def getTableEventCount(): java.util.HashMap[String, AtomicLong]
}


class MySQLExtractor(
                      host: String, port: Int, serverId: Long, username: String, password: String,
                      binlogFilename_Position: Option[(String, Long)])
  extends MySQLExtractorMBean {

  import MySQLExtractor._

  private val client = new BinaryLogClient(host, port, username, password)
  var tableEventCount = new java.util.HashMap[String, AtomicLong]()
  private var tablesById = Map[Long, TableInfo]()
  client.setServerId(serverId)
  binlogFilename_Position.foreach { case (f, p) =>
    client.setBinlogFilename(f)
    client.setBinlogPosition(p)
  }
  client.registerEventListener(new EventListener {
    override def onEvent(event: Event) {
      // client will automatically catch exception (if any) and log it out
      // Log.trace(event.toString)
      event.getData.asInstanceOf[EventData] match {
        case data: FormatDescriptionEventData =>
          onFormatDescription(data)

        case data: RotateEventData =>
          onRotate(data)

        case data: TableMapEventData =>
          onTableMap(data)

        case data: WriteRowsEventData =>
          onInsert(event.getHeader.asInstanceOf[EventHeaderV4], data)

        case data: UpdateRowsEventData =>
          onUpdate(event.getHeader.asInstanceOf[EventHeaderV4], data)

        case data: DeleteRowsEventData =>
          onRemove(event.getHeader.asInstanceOf[EventHeaderV4], data)

        case _ =>
      }
    }
  })
  //private var tablesByName = Map[String, TableInfo]()
  private var listeners = Seq[RepEvent.Listener]()

  override def getTableEventCount(): java.util.HashMap[String, AtomicLong] = {
    tableEventCount
  }

  def addListener(listener: RepEvent.Listener) {
    synchronized {
      listeners = listeners :+ listener
    }
  }

  def connectKeepAlive() {
    // https://github.com/shyiko/mysql-binlog-connector-java/issues/37
    val lifecycleListener = new LifecycleListener() {
      private var shouldReconnect = true

      override def onCommunicationFailure(client: BinaryLogClient, e: Exception) {
        if (e.getMessage == "1236 - Could not find first log file name in binary log index file") {
          Log.error(
            "Binlog {}/{} is no longer available on the master; need to rebootstrap",
            client.getBinlogFilename, client.getBinlogPosition
          )
          shouldReconnect = false
          disconnectAndExit()
        } else {
          Log.warn("Communication failure", e)
        }
      }

      override def onConnect(client: BinaryLogClient) {
        // BinaryLogClient already logs like this:
        // [INFO] Connected to localhost:3306 at mysql-bin.000003/92866
      }

      override def onEventDeserializationFailure(client: BinaryLogClient, e: Exception) {
        Log.warn("Event deserialization failure", e)
      }

      override def onDisconnect(client: BinaryLogClient) {
        if (shouldReconnect) {
          Log.warn("Disconnected; reconnect in {} seconds", RECONNECT_DELAY_SECS)
          connectInNewThread(RECONNECT_DELAY_SECS)
        } else {
          Log.warn("Disconnected; won't reconnect")
        }
      }
    }

    client.registerLifecycleListener(lifecycleListener)
    connectInNewThread(0)
  }

  def disconnect() {
    client.disconnect()
  }

  def disconnectAndExit() {
    try {
      // Need to disconnect before exiting, otherwise we can't exit because
      // there are still running threads
      client.disconnect()
    } catch {
      case NonFatal(e) => Log.warn("Could not disconnect", e)
    } finally {
      Log.info("Program should now exit")
      System.exit(-1)
    }
  }

  private def connectInNewThread(delaySecs: Long) {
    new Thread {
      override def run() {
        try {
          if (delaySecs > 0) Thread.sleep(delaySecs * 1000)
          client.connect()
        } catch {
          case NonFatal(e) =>
            val delay = if (delaySecs > 0) delaySecs else RECONNECT_DELAY_SECS
            Log.error(s"Could not connect, will reconnect in $delay seconds", e)
            connectInNewThread(delay)
        }
      }
    }.start()
  }

   private def onFormatDescription(data: FormatDescriptionEventData) {
    Log.info(
      "{}:{} server version: {}, binlog version: {}",
      host, port.toString,
      data.getServerVersion, data.getBinlogVersion.toString
    )
  }

  private def onRotate(data: RotateEventData) {
    val filename = data.getBinlogFilename
    val position = data.getBinlogPosition
    Log.info("Rotated filename: {}, position: {}", filename, position)

    synchronized {
      for (listener <- listeners) {
        listener.onEvent(new RepEvent.BinlogRotate(filename, position))
      }
    }
  }


  private def onTableMap(data: TableMapEventData) {
    //TODO: actions neended ,while table changes
    val db = data.getDatabase
    val tableId = data.getTableId
    tablesById.get(tableId) match {
      case None => {
        val tableInfo = TableInfo.get(data, host, port, username, password)
        tablesById = tablesById.updated(tableId, tableInfo)
      }
      case Some(existingTableInfo) =>
        if (!existingTableInfo.sameData(data)) {
          val oldTableInfo = existingTableInfo
          val tableInfo = TableInfo.get(data, host, port, username, password)
          //exit when pk change
          if (oldTableInfo.isKeyColumnChanged(tableInfo)) {
            throw new Exception("primary key columns changed in table : " + tableInfo.getDBTableName())
          }
          tablesById = tablesById.updated(tableId, tableInfo)
        }
    }
  }


  private def onInsert(header: EventHeaderV4, data: WriteRowsEventData) {
    val np = header.getNextPosition
    val id = data.getTableId
    val ti = tablesById.get(id)
    countEvent(ti.get, "insert")
    if (doesDbNeedRep(ti, np)) synchronized {
      for (listener <- listeners) {
        listener.onEvent(new RepEvent.Insert(np, ti.get, data))
      }
    }
  }

  private def countEvent(tableInfo: TableInfo, event: String): Unit = {
    val atype = tableInfo.getDBTableName() + "." + event
    if (!tableEventCount.containsKey(atype)) {
      tableEventCount.put(atype, new AtomicLong)
    }
    tableEventCount.get(atype).incrementAndGet()
  }

  private def doesDbNeedRep(tio: Option[TableInfo], nextPosition: Long): Boolean = {
    if (HBaseTableUtils.isTableNeedReplicated(tio.get.data.getDatabase, tio.get.getDBTableName))
      true
    else{
    synchronized {
      for (listener <- listeners) {
        listener.onEvent(new RepEvent.BinlogNextPosition(nextPosition))
      }
    }
    false
    }
  }

  private def onUpdate(header: EventHeaderV4, data: UpdateRowsEventData) {
    val np = header.getNextPosition
    val id = data.getTableId
    val ti = tablesById.get(id)
    countEvent(ti.get, "update")
    if (doesDbNeedRep(ti, np)) synchronized {
      for (listener <- listeners) {
        listener.onEvent(new RepEvent.Update(np, ti.get, data))
      }
    }
  }

  private def onRemove(header: EventHeaderV4, data: DeleteRowsEventData) {
    val np = header.getNextPosition
    val id = data.getTableId
    val ti = tablesById.get(id)
    countEvent(ti.get, "delete")
    if (doesDbNeedRep(ti, np)) synchronized {
      for (listener <- listeners) {
        listener.onEvent(new RepEvent.Remove(np, ti.get, data))
      }
    }
  }
}
