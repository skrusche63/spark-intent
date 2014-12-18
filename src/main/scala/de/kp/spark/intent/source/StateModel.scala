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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.intent.model._
import de.kp.spark.intent.spec.StateFields

class StateModel(@transient sc:SparkContext) extends Serializable {
  
  def behaviorFromElastic(req:ServiceRequest, rawset:RDD[Map[String,String]]):RDD[Behavior] = {
    
    val spec = sc.broadcast(new StateFields().get(req))
    val states = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1)
      val user = data(spec.value(Names.USER_FIELD)._1)      

      val timestamp = data(spec.value(Names.TIMESTAMP_FIELD)._1).toLong
      val state  = data(spec.value(Names.STATE_FIELD)._1)
      
      (site,user,timestamp,state)
      
    })

    statesToBehavior(states)
    
  }
  
  def observationFromElastic(req:ServiceRequest, rawset:RDD[Map[String,String]]):Array[String] = {
    
    val spec = sc.broadcast(new StateFields().get(req))
    val states = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1)
      val user = data(spec.value(Names.USER_FIELD)._1)      

      val timestamp = data(spec.value(Names.TIMESTAMP_FIELD)._1).toLong
      val state  = data(spec.value(Names.STATE_FIELD)._1)
      
      (site,user,timestamp,state)
      
    })

    statesToObservation(states)
    
  }

  def behaviorFromFile(req:ServiceRequest, rawset:RDD[String]):RDD[Behavior] = {
    
    val states = rawset.map {line =>
      
      val Array(site,user,timestamp,state) = line.split(',')
      (site,user,timestamp.toLong,state)

    }

    statesToBehavior(states)
    
  }

  def observationFromFile(req:ServiceRequest, rawset:RDD[String]):Array[String] = {
    
    val states = rawset.map {line =>
      
      val Array(site,user,timestamp,state) = line.split(',')
      (site,user,timestamp.toLong,state)

    }

    statesToObservation(states)
    
  }
  
  def behaviorFromJDBC(req:ServiceRequest, rawset:RDD[Map[String,Any]]):RDD[Behavior] = {
  
    val spec = sc.broadcast(new StateFields().get(req))
    val states = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1).asInstanceOf[String]
      val user = data(spec.value(Names.USER_FIELD)._1).asInstanceOf[String] 

      val timestamp = data(spec.value(Names.TIMESTAMP_FIELD)._1).asInstanceOf[Long]
      val state  = data(spec.value(Names.STATE_FIELD)._1).asInstanceOf[String]
      
      (site,user,timestamp,state)
      
    })

    statesToBehavior(states)
    
  }
  
  def observationFromJDBC(req:ServiceRequest, rawset:RDD[Map[String,Any]]):Array[String] = {
  
    val spec = sc.broadcast(new StateFields().get(req))
    val states = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1).asInstanceOf[String]
      val user = data(spec.value(Names.USER_FIELD)._1).asInstanceOf[String] 

      val timestamp = data(spec.value(Names.TIMESTAMP_FIELD)._1).asInstanceOf[Long]
      val state  = data(spec.value(Names.STATE_FIELD)._1).asInstanceOf[String]
      
      (site,user,timestamp,state)
      
    })

    statesToObservation(states)
    
  }
 
  def behaviorFromParquet(req:ServiceRequest, rawset:RDD[Map[String,Any]]):RDD[Behavior] = {
    
    val spec = sc.broadcast(new StateFields().get(req))
    val states = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1).asInstanceOf[String]
      val user = data(spec.value(Names.USER_FIELD)._1).asInstanceOf[String] 

      val timestamp = data(spec.value(Names.TIMESTAMP_FIELD)._1).asInstanceOf[Long]
      val state  = data(spec.value(Names.STATE_FIELD)._1).asInstanceOf[String]
      
      (site,user,timestamp,state)
      
    })

    statesToBehavior(states)
    
  }
 
  def observationFromParquet(req:ServiceRequest, rawset:RDD[Map[String,Any]]):Array[String] = {
    
    val spec = sc.broadcast(new StateFields().get(req))
    val states = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1).asInstanceOf[String]
      val user = data(spec.value(Names.USER_FIELD)._1).asInstanceOf[String] 

      val timestamp = data(spec.value(Names.TIMESTAMP_FIELD)._1).asInstanceOf[Long]
      val state  = data(spec.value(Names.STATE_FIELD)._1).asInstanceOf[String]
      
      (site,user,timestamp,state)
      
    })

    statesToObservation(states)
    
  }

  private def statesToBehavior(states:RDD[(String,String,Long,String)]):RDD[Behavior] = {
    /*
     * Group states by site & user and restrict to those
     * users with more than one purchase
     */
    states.groupBy(x => (x._1,x._2)).map(y => {

      val (site,user) = y._1
      /* Sort states by timestamp and build list of states */
      val states = y._2.toList.sortBy(_._3).map(_._4)
     
      new Behavior(site,user,states.toList)
      
    })
   
  }

  private def statesToObservation(states:RDD[(String,String,Long,String)]):Array[String] = {

    /*
     * An 'observation' is a time-ordered list of (site,user) states
     */
    val observation = states.sortBy(_._3).map(_._4).collect()
    observation
  
  }  
}