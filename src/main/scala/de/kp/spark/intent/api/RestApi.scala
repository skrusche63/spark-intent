package de.kp.spark.intent.api
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

import java.util.Date

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._

import spray.routing.{Directives,HttpService,RequestContext,Route}
import spray.routing.directives.CachingDirectives

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.core.model._
import de.kp.spark.core.rest.RestService

import de.kp.spark.intent.actor.IntentMaster
import de.kp.spark.intent.Configuration

import de.kp.spark.intent.model._


class RestApi(host:String,port:Int,system:ActorSystem,@transient val sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.core.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system
  
  val (duration,retries,time) = Configuration.actor   
  val master = system.actorOf(Props(new IntentMaster(sc)), name="intent-master")
 
  private val service = "intent"
    
  def start() {
    RestService.start(routes,system,host,port)
  }

  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {
   /*
     * A 'fields' request supports the retrieval of the field
     * or metadata specificiations that are associated with
     * a certain training task (uid).
     * 
     * The approach actually supported enables the registration
     * of field specifications on a per uid basis, i.e. each
     * task may have its own fields. Requests that have to
     * refer to the same fields must provide the SAME uid
     */
    path("fields") {  
	  post {
	    respondWithStatus(OK) {
	      ctx => doFields(ctx)
	    }
	  }
    }  ~  
    /*
     * A 'register' request supports the registration of a field
     * or metadata specification that describes the fields used
     * to span the training dataset.
     */
    path("register" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx,subject)
	    }
	  }
    }  ~ 
    /*
     * 'index' and 'track' requests refer to the tracking functionality of the
     * Intent Recognition engine; while 'index' prepares a certain Elasticsearch 
     * index, 'track' is used to gather training data.
     */
    path("index" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx,subject)
	    }
	  }
    }  ~ 
    path("track" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx,subject)
	    }
	  }
    }  ~      
    /*
     * A 'params' request supports the retrieval of the parameters
     * used for a certain model training task
     */
    path("params") {  
	  post {
	    respondWithStatus(OK) {
	      ctx => doParams(ctx)
	    }
	  }
    }  ~ 
    /*
     * A 'status' request supports the retrieval of the status
     * with respect to a certain training task (uid). The latest
     * status or all stati of a certain task are returned.
     */
    path("status" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,subject)
	    }
	  }
    }  ~ 
    path("get" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,subject)
	    }
	  }
    }  ~ 
    path("train") {
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx)
	    }
	  }
    }
    
  }

  /**
   * 'fields' and 'register' requests refer to the metadata management; 
   * for a certain task (uid) and a specific model (name), a specification 
   * of the respective data fields can be registered and retrieved from a 
   * Redis database.
   * 
   * Request parameters for the 'fields' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   */
  private def doFields[T](ctx:RequestContext) = doRequest(ctx,service,"fields")
   /**
   * Request parameters for the 'register' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - user (String)
   * - timestamp (String) 
   * - amount (Float)
   * 
   */    
  private def doRegister[T](ctx:RequestContext,subject:String) = {
	
    val task = "register:" + subject
    
    val topics = List("amount")
    if (topics.contains(subject)) doRequest(ctx,service,task)
     
  }  
  /**
   * 'index' & 'track' requests support data registration in an Elasticsearch
   * index. Request parameters for the 'index' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - index (String)
   * - type (String)
   * 
   */
  private def doIndex[T](ctx:RequestContext,subject:String) = {
	
    val task = "index:" + subject
    
    val topics = List("amount")
    if (topics.contains(subject)) doRequest(ctx,service,task)
    
  }
  /**
   * Request parameters for the 'track' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - source (String)
   * - type (String)
   * 
   * - user (String)
   * - timestamp (String) 
   * - amount (Float)
   * 
   */   
  private def doTrack[T](ctx:RequestContext,subject:String) = {
	
    val task = "track:" + subject
    
    val topics = List("amount")
    if (topics.contains(subject)) doRequest(ctx,service,task)

  }
  /**
   * Request parameters for the 'params' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   */
  private def doParams[T](ctx:RequestContext) = doRequest(ctx,service,"params")
  /**
   * 'status' is an administration request to determine whether a 
   * certain data mining task has been finished or not.
   * 
   * Request parameters for the 'status' request:
   * 
   * - site (String)
   * - uid (String)
   * 
   */
  private def doStatus[T](ctx:RequestContext,subject:String) = {
    
    val task = "status:" + subject
    /*
     * The following topics are supported:
     * 
     * Retrieve the 'latest' status information about a certain
     * data mining or model building task.
     * 
     * Retrieve 'all' stati assigned to a certain data mining
     * or model building task.
     *  
     */
    val topics = List("latest","all")
    if (topics.contains(subject)) doRequest(ctx,service,task)
  
  }
  /**
   * Request parameters for the 'get' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * and the following parameters depend on the selected topic:
   * 
   * topic: loyalty, purchase
   * 
   * - purchases (String, specification of observed purchases)
   * 
   * topic: observation
   * 
   * This topic supports the retrieval of a sequence of states
   * from a sequence of observations
   * 
   * - observations (String, comma-separated list of Strings)
   * 
   * topic: state
   * 
   * This topic supports the retrieval of a sequence of subsequent
   * states that follow a certain 'state'; the request requires the
   * number of 'steps' to look ahead and the name of the reference
   * state
   * 
   * - steps (Int)
   * - state (String)
   * 
   */    
  private def doGet[T](ctx:RequestContext,subject:String) = {
	
    val task = "get:" + subject
    
    val topics = List("loyalty","purchase","observation","state")
    if (topics.contains(subject)) doRequest(ctx,service,task)
    
  }  
  /**
   * Request parameters for the 'train' request
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - algorithm (String, MARKOV, HIDDEN_MARKOV)
   * - source (String, ELASTIC, FILE, JDBC, PIWIK)
   * 
   * - intent (String, LOYALTY, PURCHASE)
   * 
   * and the following parameters depend on the selected source:
   * 
   * ELASTIC:
   * 
   * - source.index (String)
   * - source.type (String)
   * - query (String)
   * 
   * JDBC:
   * 
   * - query (String)
   * 
   * and the following parameters depend on the selected sink:
   * 
   * ELASTIC:
   * 
   * - sink.index (String)
   * - sink.type (String)
   * 
   * and the model building parameters have to be distinguished by the
   * selected algorithm
   * 
   * MARKOV
   * 
   * HIDDEN_MARKOV
   * 
   * - epsilon (Double)
   * - iterations (Int)
   *  
   */    
  private def doTrain[T](ctx:RequestContext) = doRequest(ctx,service,"train")
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String) = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master,request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }

}