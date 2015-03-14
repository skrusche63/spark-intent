package de.kp.spark.intent

import java.io.{ObjectInputStream,ObjectOutputStream} 

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem,Path}

import de.kp.scala.hmm.HmmModel
import de.kp.spark.intent.markov.{HiddenMarkovModel,TransitionMatrix}

import org.apache.mahout.math.{DenseMatrix,DenseVector}

private class HMStruct(
  val hstates:Map[Int,String],
  val ostates:Map[String,Int],
  val A:DenseMatrix,
  val B:DenseMatrix,
  val Pi:DenseVector
)

private class TMStruct(
  val scale:Int,
  val states:Array[String],
  val matrix:TransitionMatrix
)

object MarkovIO {

  def readHM(store:String):HiddenMarkovModel = {
    
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)
    
    val ois = new ObjectInputStream(fs.open(new Path(store)))
    val struct = ois.readObject().asInstanceOf[HMStruct]
      
    ois.close()
    
    val hstates = struct.hstates
    val ostates = struct.ostates
    
    val A = struct.A
    val B = struct.B
    
    val Pi = struct.Pi
    
    new HiddenMarkovModel(hstates,ostates,new HmmModel(A,B,Pi))
    
  }
  /**
   * Read Markov (chain) transition matrix with assigned
   * scale and states parameters from HDFS
   */
  def readTM(store:String):(Int,Array[String],TransitionMatrix) = {
    
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)
    
    val ois = new ObjectInputStream(fs.open(new Path(store)))
    val struct = ois.readObject().asInstanceOf[TMStruct]
      
    ois.close()
    
    val scale = struct.scale
    val states = struct.states
    
    val matrix = struct.matrix
    
    (scale,states,matrix)
    
  }
  
  def writeHM(store:String,model:HiddenMarkovModel) {
   
    val hstates = model.hstates
    val ostates = model.ostates
    
    val hmm = model.hmm
    
    val A = hmm.getAMatrix
    val B = hmm.getBMatrix
    
    val Pi = hmm.getPiVector
    
    val struct = new HMStruct(hstates,ostates,A,B,Pi)
        
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)

    val oos = new ObjectOutputStream(fs.create(new Path(store)))   
    oos.writeObject(struct)
    
    oos.close
    
  }
  
  /**
   * Write Markov (chain) transition matrix with assigned
   * scale and states parameters to HDFS
   */
  def writeTM(store:String,scale:Int,states:Array[String],matrix:TransitionMatrix) {
 
    val struct = new TMStruct(scale,states,matrix)
        
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)

    val oos = new ObjectOutputStream(fs.create(new Path(store)))   
    oos.writeObject(struct)
    
    oos.close
 
  }
  
}