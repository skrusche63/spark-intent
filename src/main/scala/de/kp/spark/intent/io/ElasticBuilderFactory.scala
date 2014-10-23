package de.kp.spark.intent.io
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
import org.elasticsearch.common.xcontent.XContentBuilder
  
object ElasticBuilderFactory {
  /*
   * Definition of common parameters for all indexing tasks
   */
  val TIMESTAMP_FIELD:String = "timestamp"

  /******************************************************************
   *                          AMOUNT
   *****************************************************************/

  val SITE_FIELD:String = "site"
  val USER_FIELD:String = "user"

  val AMOUNT_FIELD:String = "amount"

  def getBuilder(builder:String,mapping:String):XContentBuilder = {
    
    builder match {
      /*
       * Elasticsearch is used to track amount-based events to provide
       * a sequential database within an Elasticsearch index
       */
      case "amount" => new ElasticAmountBuilder().createBuilder(mapping)
      
      case _ => null
      
    }
  
  }

}