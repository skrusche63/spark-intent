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

import de.kp.spark.intent.Configuration
import de.kp.spark.intent.io.JdbcReader

import de.kp.spark.intent.model._
import de.kp.spark.intent.spec.Fields

class JdbcSource(@transient sc:SparkContext) {
   
  protected val (url,database,user,password) = Configuration.mysql
  
  def connect(params:Map[String,Any] = Map.empty[String,Any]):RDD[Map[String,Any]] = {
    
    val uid = params("uid").asInstanceOf[String]    
    
    val fieldspec = Fields.get(uid,Intents.LOYALTY)
    val fields = fieldspec.map(kv => kv._2._1).toList
    
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]

    new JdbcReader(sc,site,query).read(fields)
  
  }

}