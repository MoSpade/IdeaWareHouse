package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def load(propertiesName: String): Properties = {
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    prop
  }

  def main(args: Array[String]): Unit = {
    val properties: Properties = load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

}
