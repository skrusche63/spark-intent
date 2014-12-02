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

object Fields {

  val loyalty_xml:String  = "loyalty_fields.xml"
  val purchase_xml:String = "purchase_fields.xml"

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)
  
  def get(req:ServiceRequest,intent:String):Map[String,(String,String)] = {
    
    val fields = HashMap.empty[String,(String,String)]

    try {
          
      if (cache.fieldsExist(req)) {      
        
        val fieldspec = cache.fields(req)
        for (field <- fieldspec) {
        
          val _name = field.name
          val _type = field.datatype
          
          val _mapping = field.value
          fields += _name -> (_mapping,_type) 
          
        }
    
      } else {
        /*
         * In case of no dynamic metadata provided, the field specification
         * is retrieved from pre-defined xml files
         */
        val root = intent match {
        
          case Intents.LOYALTY => {
            XML.load(getClass.getClassLoader.getResource(loyalty_xml))           
          }
        
          case Intents.PURCHASE => {
            XML.load(getClass.getClassLoader.getResource(purchase_xml))  
          }
        
          case _ => null
        
        }
        
        if (root == null) throw new Exception("Intent is unknown.")
      
        for (field <- root \ "field") {
      
          val _name  = (field \ "@name").toString
          val _type  = (field \ "@type").toString

          val _mapping = field.text
          fields += _name -> (_mapping,_type) 
      
        }
        
      }
      
    } catch {
      case e:Exception => {}
    }
    
    fields.toMap
  }
    
}