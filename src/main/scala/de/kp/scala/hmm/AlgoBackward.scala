package de.kp.scala.hmm
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Scala-HMM project
* (https://github.com/skrusche63/scala-hmm).
* 
* Scala-HMM is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Scala-HMM is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Scala-HMM. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.mahout.math.{DenseMatrix,DenseVector}

/**
 * Implementation of the Backward algorithm
 */
object AlgoBackward {

  def run(model:HmmModel, observations:Array[Int], scaled:Boolean):DenseMatrix = {

    val beta = new DenseMatrix(observations.length, model.getNumHiddenStates())
    run(model, beta, observations, scaled)

    beta
    
  }

  def run(model:HmmModel,beta:DenseMatrix,observations:Array[Int],scaled:Boolean) {
    
    val A = model.getAMatrix
    val B = model.getBMatrix

    val numStates = model.getNumHiddenStates()
    val numObserv = observations.length

    if (scaled) {
      
      /* 
       * Initialization
       */
      (0 until numStates).foreach(i => beta.setQuick(numObserv-1,i,0))

      for (t <- numObserv - 2 to 0 by -1) {
        (0 until numStates).foreach(i => {
        
          var sum = Double.NegativeInfinity // log(0)
          (0 until numStates).foreach(j => {
            
            val tmp = beta.getQuick(t + 1, j) + Math.log(A.getQuick(i,j)) + Math.log(B.getQuick(j, observations(t + 1)))
            if (tmp > Double.NegativeInfinity) {
              // handle log(0)
              sum = tmp + Math.log1p(Math.exp(sum - tmp))
            }
          })
          
          beta.setQuick(t, i, sum)
        
        })
      }
      
    } else {
      /* 
       * Initialization
       */
      (0 until numStates).foreach(i => beta.setQuick(numObserv-1,i,1))
      
      for (t <- numObserv - 2 to 0 by -1) {
        (0 until numStates).foreach(i => {
          
          var sum = 0.0
          (0 until numStates).foreach(j => sum += beta.getQuick(t+1,j) * A.getQuick(i,j) * B.getQuick(j, observations(t+1)))          
          beta.setQuick(t,i, sum)
        
        })
      
      }
    
    }
  
  }
  
}