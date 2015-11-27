package mysql2hbase

import java.io._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by CM on 2015/8/24.
 *
 */
object SerializableUtil {
//  lazy val kryo = new Kryo();

  def serializableToBytes(value: Serializable): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    try{
      out.writeObject(value)
      bos.toByteArray
    }finally {
      out.close()
      bos.close()
    }
  }


  def serializableToBytes(values: ArrayBuffer[Serializable]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    try{
      for(value <- values){
        out.writeObject(value)
      }
      bos.toByteArray
    }finally {
      out.close()
      bos.close()
    }
  }

  def bytesToObjects(b:Array[Byte]): Array[Object] ={
    val bis = new ByteArrayInputStream(b)
    val in = new ObjectInputStream(bis)
    val ret = ArrayBuffer[Object]()
    var obj:Object = null
    try {
    while(  (obj=in.readObject()) !=null ){
        ret.append(obj)
      }
    }
    catch{
      case (e:EOFException)=> {obj=null}
    }finally {
      in.close()
      bis.close()
    }
    ret.toArray
  }
}
