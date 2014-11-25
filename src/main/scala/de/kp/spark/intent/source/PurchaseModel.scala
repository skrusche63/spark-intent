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

import de.kp.spark.core.model._
import de.kp.spark.intent.model._

import de.kp.spark.intent.state.PurchaseState
import de.kp.spark.intent.spec.Fields

import scala.collection.mutable.ArrayBuffer

class PurchaseModel(@transient sc:SparkContext) extends PurchaseState with Serializable {
 
  def buildElastic(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[Behavior] = {
    
    val spec = sc.broadcast(Fields.get(req,Intents.PURCHASE))
    val purchases = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val user = data(spec.value("user")._1)      

      val timestamp = data(spec.value("timestamp")._1).toLong
      val amount  = data(spec.value("amount")._1).toFloat
      
      new Purchase(site,user,timestamp,amount)
      
    })
   
    behaviors(purchases)
    
  }

  def buildFile(req:ServiceRequest,rawset:RDD[String]):RDD[Behavior] = {
    
    val purchases = rawset.map {line =>
      
      val Array(site,user,timestamp,amount) = line.split(',')
      new Purchase(site,user,timestamp.toLong,amount.toFloat)

    }
   
    behaviors(purchases)
    
  }
 
  def buildJDBC(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[Behavior] = {
    
    val fieldspec = Fields.get(req,Intents.PURCHASE)
    val fields = fieldspec.map(kv => kv._2._1).toList
  
    val spec = sc.broadcast(fieldspec)
    val purchases = rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val user = data(spec.value("user")._1).asInstanceOf[String] 

      val timestamp = data(spec.value("timestamp")._1).asInstanceOf[Long]
      val amount  = data(spec.value("amount")._1).asInstanceOf[Float]
      
      new Purchase(site,user,timestamp,amount)
      
    })
   
    behaviors(purchases)
    
  }
 
  def buildPiwik(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[Behavior] = {
    
    val purchases = rawset.map(row => {
      
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
   
    behaviors(purchases)
    
  }

  /**
   * Represent transactions as a time ordered sequence of Markov States;
   * the result is directly used to build the respective Markov Model
   */
  def behaviors(sequences:RDD[Purchase]):RDD[Behavior] = {
    
    /*
     * Group purchases by site & user and restrict to those
     * users with more than one purchase
     */
    sequences.groupBy(p => (p.site,p.user)).filter(_._2.size > 1).map(p => {

      val (site,user) = p._1
      val orders      = p._2.map(v => (v.timestamp,v.amount)).toList.sortBy(_._1)
      
      /* Extract first order */
      var (pre_time,pre_amount) = orders.head
      val states = ArrayBuffer.empty[String]

      for ((time,amount) <- orders.tail) {
        
        /* Determine state from amount */
        val astate = stateByAmount(amount,pre_amount)
     
        /* Determine state from time elapsed between
         * subsequent orders or transactions
         */
        val tstate = stateByTime(time,pre_time)
      
        val state = astate + tstate
        states += state
        
        pre_amount = amount
        pre_time   = time
        
      }
      
      new Behavior(site,user,states.toList)
      
    })
    
  }
}