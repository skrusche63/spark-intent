package de.kp.spark.intent.markov.hadoop
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import org.apache.hadoop.conf.{Configuration => HadoopConf}

import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.io.{SequenceFile,Text}

import org.apache.mahout.math.{DenseMatrix,DenseVector}

import de.kp.scala.hmm.HmmModel

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object HadoopIO {
  
  def writeHMM(hStates:Map[Int,String],oStates:Map[String,Int],model:HmmModel,path:String) {
    
    /* Write states */
    writeToHadoop(serializeHStates(hStates), path + "/hstates")
    writeToHadoop(serializeOStates(oStates), path + "/ostates")

    /* A Matrix */    
    writeToHadoop(serializeDM(model.getAMatrix), path + "/a-matrix")

    /* B Matrix */    
    writeToHadoop(serializeDM(model.getBMatrix), path + "/b-matrix")
    
    /* Pi Vector */
    writeToHadoop(serializeDV(model.getPiVector), path + "/pi-vector")

  }

  def readHMM(path:String):(Map[Int,String],Map[String,Int],HmmModel) = {
    
    /* Read states */
    val hStates = deserializeHStates(readFromHadoop(path + "/hstates"))
    val oStates = deserializeOStates(readFromHadoop(path + "/ostates"))

    /*
     * Read A & B matrix
     */
    val A = deserializeDM(readFromHadoop(path + "/a-matrix"))
    val B = deserializeDM(readFromHadoop(path + "/b-matrix"))

    /*
     * Pi Vector
     */
    val Pi = deserializeDV(readFromHadoop(path + "/pi-vector"))
    
    /* Build model */
    val model = new HmmModel(A,B,Pi)
    
    (hStates,oStates,model)
  
  }

  private def deserializeHStates(hStates:String):Map[Int,String] = {

    implicit val formats = Serialization.formats(NoTypeHints)
	read[Map[Int,String]](hStates)
    
  }
  
  private def serializeHStates(hStates:Map[Int,String]):String = {

    implicit val formats = Serialization.formats(NoTypeHints)
	write(hStates)
    
  }

  private def deserializeOStates(oStates:String):Map[String,Int] = {

    implicit val formats = Serialization.formats(NoTypeHints)
	read[Map[String,Int]](oStates)
    
  }

  private def serializeOStates(oStates:Map[String,Int]):String = {

    implicit val formats = Serialization.formats(NoTypeHints)
	write(oStates)
    
  }

  private def serializeDM(matrix:DenseMatrix):String = {
    
    val values = ArrayBuffer.empty[String]
    
    val rows = matrix.rowSize
    for (i <- 0 until rows) {
      
      val vector = matrix.viewRow(i)
      val row = ArrayBuffer.empty[Double]

      for (element <- vector.all) {
        row += element.get()      
      }
      
      values += row.mkString(",")
    
    }

    values.mkString(";")
    
  }

  private def deserializeDM(matrix:String):DenseMatrix = {
    
    val rows = matrix.split(";")    
    
    val rdim = rows.length;    
    val cdim = rows(0).split(",").length
    
    val values = Array.fill[Double](rdim,cdim)(0)
    
    (0 until rdim).foreach(i => {
      values(i) = rows(i).split(",").map(_.toDouble)
    })
    
    new DenseMatrix(values)
    
  }

  private def serializeDV(vector:DenseVector):String = {
    
    val values = ArrayBuffer.empty[Double]

    val iterator= vector.iterator()
    for (element <- iterator) {       
      values += element.get()      
    }
    
    values.mkString(",")
    
  }

  private def deserializeDV(vector:String):DenseVector = {
    
    val values = vector.split(",").map(_.toDouble)
    new DenseVector(values)
    
  }
  
  private def writeToHadoop(ser:String,file:String) {

    try {
		
      val conf = new HadoopConf()
	  val fs = FileSystem.get(conf)

      val path = new Path(file)
	  val writer = new SequenceFile.Writer(fs, conf, path, classOf[Text], classOf[Text])

	  val k = new Text()
	  val v = new Text(ser)

	  writer.append(k,v)
	  writer.close()

	} catch {
	  case e:Exception => throw new Exception(e.getMessage())

	}
 
  }
  
  private def readFromHadoop(file:String):String = {
    
    try {
		
      val conf = new HadoopConf()
	  val fs = FileSystem.get(conf)

      val path = new Path(file)
      
      val reader = new SequenceFile.Reader(fs,path,conf)

      val k = new Text()
      val v = new Text()

      reader.next(k, v)
      reader.close()
      
      v.toString

	} catch {
	  case e:Exception => throw new Exception(e.getMessage())

	}

  }
  
}
