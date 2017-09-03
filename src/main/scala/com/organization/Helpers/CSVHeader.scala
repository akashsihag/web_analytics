package com.organization.Helpers

/**
  * @author ${Akash.Sihag}
  */
class CSVHeader(arrayHeader: Array[String]) extends Serializable {

  val index = arrayHeader.zipWithIndex.toMap

  def apply(array: Array[String], key: String): String = {
    try {
      array(index(key))
    }
    catch {
      case e: ArrayIndexOutOfBoundsException => ""
    }
  }
}

