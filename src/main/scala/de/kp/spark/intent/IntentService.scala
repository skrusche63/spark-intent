package de.kp.spark.intent
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

import akka.actor.{ActorSystem,Props}
import com.typesafe.config.ConfigFactory

import de.kp.spark.core.SparkService
import de.kp.spark.intent.actor.IntentMaster

object IntentService {

  def main(args: Array[String]) {
    
    val name:String = "intent-server"
    val conf:String = "server.conf"

    val server = new IntentService(conf, name)
    while (true) {}
    
    server.shutdown
      
  }

}

class IntentService(conf:String, name:String) extends SparkService {

  val system = ActorSystem(name, ConfigFactory.load(conf))
  sys.addShutdownHook(system.shutdown)
  
  /* Create Spark context */
  private val sc = createCtxLocal("IntentContext",Configuration.spark)      

  val master = system.actorOf(Props[IntentMaster], name="intent-master")

  def shutdown = system.shutdown()
  
}