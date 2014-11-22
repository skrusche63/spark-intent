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

import de.kp.spark.core.source.{ElasticSource,JdbcSource}

import de.kp.spark.intent.model._
import de.kp.spark.intent.spec.Fields

class LoyaltySource(@transient sc:SparkContext) {

  private val model = new LoyaltyModel(sc)
  
  def get(data:Map[String,String]):Array[String] = {

    val uid = data("uid")
    
    val source = data("source")
    source match {

      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(data)    
        model.buildElastic(uid,rawset)

      }

      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(data)
        model.buildFile(uid,rawset)
        
      }

      case Sources.JDBC => {
    
        val fieldspec = Fields.get(uid,Intents.LOYALTY)
        val fields = fieldspec.map(kv => kv._2._1).toList
        
        val rawset = new JdbcSource(sc).connect(data,fields)
        model.buildJDBC(uid,rawset)
        
      }

      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(data)
        model.buildPiwik(uid,rawset)
        
      }
            
      case _ => null
      
    }
    
  }

  def scaleDef = model.scaleDef
  
  def stateDefs = model.stateDefs
  
  def hiddenDefs = model.hiddenDefs
  
}