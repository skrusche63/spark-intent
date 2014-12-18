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

import de.kp.spark.intent.spec.Fields

import scala.xml._
import scala.collection.mutable.HashMap

class PurchaseFields extends Fields {

  val purchase_xml:String = "purchase_fields.xml"

  override def fromXML:Map[String,(String,String)] = {
     
    val fields = HashMap.empty[String,(String,String)]
    /*
     * In case of no dynamic metadata provided, the field specification
     * is retrieved from pre-defined xml files
     */
    val root = XML.load(getClass.getClassLoader.getResource(purchase_xml))           
        
    if (root == null) throw new Exception("Intent is unknown.")
      
    for (field <- root \ "field") {
      
      val _name  = (field \ "@name").toString
      val _type  = (field \ "@type").toString

      val _mapping = field.text
      fields += _name -> (_mapping,_type) 
      
    }

    fields.toMap
  
  }

}