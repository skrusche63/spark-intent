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

import de.kp.spark.intent.io.ElasticReader

import de.kp.spark.intent.model._
import de.kp.spark.intent.spec.Fields

class ElasticSource(@transient sc:SparkContext) extends Source(sc) {
  
 override def loyalty(params:Map[String,Any] = Map.empty[String,Any]):Array[String] = {
    
    val query = params("query").asInstanceOf[String]
    val resource = params("resource").asInstanceOf[String]

    val uid = params("uid").asInstanceOf[String]    
    val spec = sc.broadcast(Fields.get(uid,Intents.LOYALTY))

    /* 
     * Connect to Elasticsearch and extract the following fields from the
     * respective search index: site, user, group and item
     */
    val rawset = new ElasticReader(sc,resource,query).read
    val purchases = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val user = data(spec.value("user")._1)      

      val timestamp = data(spec.value("timestamp")._1).toLong
      val amount  = data(spec.value("amount")._1).toFloat
      
      new Purchase(site,user,timestamp,amount)
      
    })
    
    new LoyaltyModel().observations(purchases)
 
 }
  
  override def purchases(params:Map[String,Any]):RDD[Behavior] = {
    
    val query = params("query").asInstanceOf[String]
    val resource = params("resource").asInstanceOf[String]

    val uid = params("uid").asInstanceOf[String]    
    val spec = sc.broadcast(Fields.get(uid,Intents.PURCHASE))

    /* 
     * Connect to Elasticsearch and extract the following fields from the
     * respective search index: site, user, group and item
     */
    val rawset = new ElasticReader(sc,resource,query).read
    val purchases = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val user = data(spec.value("user")._1)      

      val timestamp = data(spec.value("timestamp")._1).toLong
      val amount  = data(spec.value("amount")._1).toFloat
      
      new Purchase(site,user,timestamp,amount)
      
    })
   
    new PurchaseModel().behaviors(purchases)
    
  }

}