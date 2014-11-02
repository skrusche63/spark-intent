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
          
      if (RedisCache.fieldsExist(uid)) {      
        
        val fieldspec = RedisCache.fields(uid)
        for (field <- fieldspec.items) {
        
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