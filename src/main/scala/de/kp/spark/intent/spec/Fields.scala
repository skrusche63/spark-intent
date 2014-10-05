package de.kp.spark.intent.spec

import de.kp.spark.intent.model._
import de.kp.spark.intent.redis.RedisCache

import scala.xml._
import scala.collection.mutable.HashMap

object Fields {

  val loyalty_xml:String  = "loyalty_fields.xml"
  val purchase_xml:String = "purchase_fields.xml"

  def get(uid:String,intent:String):Map[String,(String,String)] = {
    
    val fields = HashMap.empty[String,(String,String)]

    try {
          
      val root = if (RedisCache.metaExists(uid)) {      
        XML.load(RedisCache.meta(uid))
    
      } else {
        /*
         * In case of no dynamic metadata provided, the field specification
         * is retrieved from pre-defined xml files
         */
        intent match {
        
          case Intents.LOYALTY => {
            XML.load(getClass.getClassLoader.getResource(loyalty_xml))           
          }
        
          case Intents.PURCHASE => {
            XML.load(getClass.getClassLoader.getResource(purchase_xml))  
          }
        
          case _ => null
        
        }
     }
   
      for (field <- root \ "field") {
      
        val _name  = (field \ "@name").toString
        val _type  = (field \ "@type").toString

        val _mapping = field.text
        fields += _name -> (_mapping,_type) 
      
      }
      
    } catch {
      case e:Exception => {}
    }
    
    fields.toMap
  }
    
}