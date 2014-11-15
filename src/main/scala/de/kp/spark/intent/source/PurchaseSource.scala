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

class PurchaseSource(@transient sc:SparkContext) {

  private val model = new PurchaseModel(sc)
  
  def get(data:Map[String,String]):RDD[Behavior] = {

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
        
        val rawset = new JdbcSource(sc).connect(data)
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
  
}