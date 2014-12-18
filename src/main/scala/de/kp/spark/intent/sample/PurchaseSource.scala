package de.kp.spark.intent.sample
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
import de.kp.spark.core.source._

import de.kp.spark.intent.Configuration

import de.kp.spark.intent.model._
import de.kp.spark.intent.source._

class PurchaseSource(@transient sc:SparkContext) {

  private val config = Configuration
  private val model = new PurchaseModel(sc)
  
  def get(req:ServiceRequest):RDD[Behavior] = {

    val uid = req.data("uid")

    val source = req.data("source")
    source match {

      case Sources.ELASTIC => {        

        val rawset = new ElasticSource(sc).connect(config,req)
        model.buildElastic(req,rawset)        
      
      }

      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(config.input(0),req)
        model.buildFile(req,rawset)        
        
      }

      case Sources.JDBC => {
    
        val fieldspec = new PurchaseFields().get(req)
        val fields = fieldspec.map(kv => kv._2._1).toList
        
        val rawset = new JdbcSource(sc).connect(config,req,fields)
        model.buildJDBC(req,rawset)        
        
      }

      case Sources.PARQUET => {
    
        val fieldspec = new PurchaseFields().get(req)
        val fields = fieldspec.map(kv => kv._2._1).toList
        
        val rawset = new ParquetSource(sc).connect(config.input(0),req,fields)
        model.buildParquet(req,rawset)        
        
      }

      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(config,req)
        model.buildPiwik(req,rawset)        
        
      }
            
      case _ => null
      
    }
    
  }

  def scale = model.scale
  
  def ostates = model.ostates

}