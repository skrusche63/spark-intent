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
import de.kp.spark.core.source._

import de.kp.spark.intent.Configuration
import de.kp.spark.intent.model._

import de.kp.spark.intent.spec.StateFields

class StateSource(@transient sc:SparkContext) {

  private val config = Configuration
  private val model = new StateModel(sc)
  
  def getAsBehavior(req:ServiceRequest):RDD[Behavior] = {
   
    val source = req.data(Names.REQ_SOURCE)
    source match {

      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(config,req)    
        model.behaviorFromElastic(req,rawset)

      }

      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(config.input(0),req)
        model.behaviorFromFile(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fieldspec = new StateFields().get(req)
        val fields = fieldspec.map(kv => kv._2._1).toList
        
        val rawset = new JdbcSource(sc).connect(config,req,fields)
        model.behaviorFromJDBC(req,rawset)
        
      }

      case Sources.PARQUET => {
    
        val fieldspec = new StateFields().get(req)
        val fields = fieldspec.map(kv => kv._2._1).toList
        
        val rawset = new ParquetSource(sc).connect(config.input(0),req,fields)
        model.behaviorFromParquet(req,rawset)
        
      }
            
      case _ => null
      
    }
    
  }
  
  def getAsObservation(req:ServiceRequest):Array[String] = {
   
    val source = req.data(Names.REQ_SOURCE)
    source match {

      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(config,req)    
        model.observationFromElastic(req,rawset)

      }

      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(config.input(0),req)
        model.observationFromFile(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fieldspec = new StateFields().get(req)
        val fields = fieldspec.map(kv => kv._2._1).toList
        
        val rawset = new JdbcSource(sc).connect(config,req,fields)
        model.observationFromJDBC(req,rawset)
        
      }

      case Sources.PARQUET => {
    
        val fieldspec = new StateFields().get(req)
        val fields = fieldspec.map(kv => kv._2._1).toList
        
        val rawset = new ParquetSource(sc).connect(config.input(0),req,fields)
        model.observationFromParquet(req,rawset)
        
      }
            
      case _ => null
      
    }
    
  }

}