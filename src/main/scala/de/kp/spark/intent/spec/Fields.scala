package de.kp.spark.intent.spec
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

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisCache

import de.kp.spark.intent.Configuration
import de.kp.spark.intent.model._

import scala.xml._
import scala.collection.mutable.HashMap

class Fields {
    
  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)
  
  def get(req:ServiceRequest):Map[String,(String,String)] = {

    try {
    
      val fields = HashMap.empty[String,(String,String)]
          
      if (cache.fieldsExist(req)) {      
        
        val fieldspec = cache.fields(req)
        for (field <- fieldspec) {
        
          val _name = field.name
          val _type = field.datatype
          
          val _mapping = field.value
          fields += _name -> (_mapping,_type) 
          
        }
    
        fields.toMap
        
      } else {
        fromXML
        
      }
      
    } catch {
      case e:Exception => Map.empty[String,(String,String)]
    }
    
  }
  
  def fromXML:Map[String,(String,String)] = null
  
}