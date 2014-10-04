package de.kp.spark.intent.source
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Intent project
* (https://github.com/skrusche63/spark-intent).
* 
* Spark-Intent is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Intent is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Intent. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.intent.model._
import de.kp.spark.intent.io.JdbcReader

/**
 * PiwikSource is an extension of the common JdbcSource that holds Piwik specific
 * data about fields and types on the server side for convenience.
 */
class PiwikSource(@transient sc:SparkContext) extends JdbcSource(sc) {

  private val LOG_FIELDS = List(
      "idsite",
      "idvisitor",
      "server_time",
      "location_country",
      "location_region",
      "location_city",
      "location_latitude",
      "location_longitude",
      "idgoal",
      "idorder",
      "items",
      "revenue",
      "revenue_subtotal",
      "revenue_tax",
      "revenue_shipping",
      "revenue_discount")
  
 override def loyalty(params:Map[String,Any] = Map.empty[String,Any]):Array[String] = {

    /* Retrieve site, start & end date from params */
    val site = params("site").asInstanceOf[Int]
    
    val startdate = params("startdate").asInstanceOf[String]
    val enddate   = params("enddate").asInstanceOf[String]

    val sql = query(database,site.toString,startdate,enddate)
    
    val rawset = new JdbcReader(sc,site,sql).read(LOG_FIELDS)    
    val purchases = rawset.filter(row => (isOrder(row) == false)).map(row => {
      
      val site = row("idsite").asInstanceOf[Long]
      /* Convert 'idvisitor' into a HEX String representation */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)
      
      /* Convert 'server_time' into universal timestamp */
      val server_time = row("server_time").asInstanceOf[java.sql.Timestamp]
      val timestamp = server_time.getTime()

      val amount = row("revenue").asInstanceOf[Float]
      
      new Purchase(site.toString,user,timestamp,amount)
      
    })
    
    new LoyaltyModel().observations(purchases)
    
  }
  
  override def purchases(params:Map[String,Any]):RDD[Behavior] = {

    /* Retrieve site, start & end date from params */
    val site = params("site").asInstanceOf[Int]
    
    val startdate = params("startdate").asInstanceOf[String]
    val enddate   = params("enddate").asInstanceOf[String]

    val sql = query(database,site.toString,startdate,enddate)
    
    val rawset = new JdbcReader(sc,site,sql).read(LOG_FIELDS)    
    val purchases = rawset.filter(row => (isOrder(row) == false)).map(row => {
      
      val site = row("idsite").asInstanceOf[Long]
      /* Convert 'idvisitor' into a HEX String representation */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)
      
      /* Convert 'server_time' into universal timestamp */
      val server_time = row("server_time").asInstanceOf[java.sql.Timestamp]
      val timestamp = server_time.getTime()

      val amount = row("revenue").asInstanceOf[Float]
      
      new Purchase(site.toString,user,timestamp,amount)
      
    })
   
    new PurchaseModel().behaviors(purchases)

  }

  /**
   * A conversion entry is specified as an ecomerce order, if the idgoal value is '0'
   */
  private def isOrder(row:Map[String,Any]):Boolean = {
    
    val idgoal = row("idgoal").asInstanceOf[Int]
    (idgoal == 0)
    
  }
  
  /*
   * Table: piwik_log_conversion
   */
  private def query(database:String,site:String,startdate:String,enddate:String) = String.format("""
    SELECT * FROM %s.piwik_log_conversion WHERE idsite >= %s AND idsite <= %s AND server_time > '%s' AND server_time < '%s'
    """.stripMargin, database, site, site, startdate, enddate) 

}