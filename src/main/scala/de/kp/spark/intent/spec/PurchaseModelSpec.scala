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

import scala.xml._
import scala.collection.mutable.HashMap

object PurchaseModelSpec {
  
  val path = "purchase.xml"
  val root:Elem = XML.load(getClass.getClassLoader.getResource(path))  

  private val data = HashMap.empty[String,String]
  
  load()
  
  private def load() {
    
    /*
     * Time settings for horizon & threshold
     */
    val thoriz_small = (root \ "time" \ "horizon" \ "small").text.trim()
    data += "time.horizon.small" -> thoriz_small

    val thoriz_medium = (root \ "time" \ "horizon" \ "medium").text.trim()
    data += "time.horizon.medium" -> thoriz_medium

    val thoriz_large = (root \ "time" \ "horizon" \ "large").text.trim()
    data += "time.horizon.large" -> thoriz_large

    val tthres_small = (root \ "time" \ "threshold" \ "small").text.trim()
    data += "time.threshold.small" -> tthres_small

    val tthres_medium = (root \ "time" \ "threshold" \ "medium").text.trim()
    data += "time.threshold.medium" -> tthres_medium

    /*
     * Amount settings for horizon & threshold
     */
    val ahoriz_less = (root \ "amount" \ "horizon" \ "less").text.trim()
    data += "amount.horizon.less" -> ahoriz_less

    val ahoriz_equal = (root \ "amount" \ "horizon" \ "equal").text.trim()
    data += "amount.horizon.equal" -> ahoriz_equal

    val ahoriz_large = (root \ "amount" \ "horizon" \ "large").text.trim()
    data += "amount.horizon.large" -> ahoriz_large

    val athres_less = (root \ "amount" \ "threshold" \ "less").text.trim()
    data += "amount.threshold.less" -> athres_less

    val athres_equal = (root \ "amount" \ "threshold" \ "equal").text.trim()
    data += "amount.threshold.equal" -> athres_equal

  }

  def get = data.toMap

}