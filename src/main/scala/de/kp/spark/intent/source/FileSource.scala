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
import de.kp.spark.intent.model._

class FileSource(@transient sc:SparkContext) extends Source(sc) {

  val input = Configuration.file()
  
 override def loyalty(params:Map[String,Any] = Map.empty[String,Any]):Array[String] = {    

    val purchases = sc.textFile(input).map {line =>
      
      val Array(site,user,timestamp,amount) = line.split(',')
      new Purchase(site,user,timestamp.toLong,amount.toFloat)

    }
    
    new LoyaltyModel().observations(purchases)
    
  }
  
  override def purchases(params:Map[String,Any]):RDD[Behavior] = {

    val purchases = sc.textFile(input).map {line =>
      
      val Array(site,user,timestamp,amount) = line.split(',')
      new Purchase(site,user,timestamp.toLong,amount.toFloat)

    }
   
    new PurchaseModel().behaviors(purchases)

  }
  
}