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
import de.kp.spark.core.source.{ElasticSource,FileSource,JdbcSource}

import de.kp.spark.intent.Configuration
import de.kp.spark.intent.model._

import de.kp.spark.intent.spec.Fields

class LoyaltySource(@transient sc:SparkContext) {

  private val model = new LoyaltyModel(sc)
  
  def get(req:ServiceRequest):Array[String] = {

    val uid = req.data("uid")
    
    val source = req.data("source")
    source match {

      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(req.data)    
        model.buildElastic(req,rawset)

      }

      case Sources.FILE => {

        val path = Configuration.file()  
        
        val rawset = new FileSource(sc).connect(req.data,path)
        model.buildFile(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fieldspec = Fields.get(req,Intents.LOYALTY)
        val fields = fieldspec.map(kv => kv._2._1).toList
        
        val rawset = new JdbcSource(sc).connect(req.data,fields)
        model.buildJDBC(req,rawset)
        
      }

      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(req.data)
        model.buildPiwik(req,rawset)
        
      }
            
      case _ => null
      
    }
    
  }

  def scaleDef = model.scaleDef
  
  def stateDefs = model.stateDefs
  
  def hiddenDefs = model.hiddenDefs
  
}