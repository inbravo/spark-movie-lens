package com.inbravo.mr.http

import com.twitter.finagle.builder.{ Server, ServerBuilder }
import com.twitter.finagle.http._
import com.twitter.finagle.{ Service, SimpleFilter }
import com.twitter.util.Future
import java.net.InetSocketAddress
import com.inbravo.mr.config.ProjectConfig

/**
 * amit.dixit
 *
 * Http service to host Movie recommendations
 */
object FinagleHttpService extends ProjectConfig {

  /**
   * A simple Filter that catches exceptions and converts them to appropriate
   * HTTP responses.
   */
  class HandleExceptions extends SimpleFilter[Request, Response] {

    def apply(request: Request, service: Service[Request, Response]) = {

      /* Asynchronously handles exceptions */
      service(request) handle {

        case error =>

          val statusCode = error match {
            
            case _: IllegalArgumentException =>
              Status.Forbidden
            case _ =>
              Status.InternalServerError
          }

          val errorResponse = Response(Version.Http11, statusCode)
          errorResponse.contentString = error.getStackTrace.mkString("\n")

          errorResponse
      }
    }
  }

  /**
   * A simple Filter that checks that the request is valid by inspecting the
   * "Authorization" header.
   */
  class Authorize extends SimpleFilter[Request, Response] {

    def apply(request: Request, continue: Service[Request, Response]) = {

      if (Some("open sesame") == request.headerMap.get(Fields.Authorization)) {

        continue(request)
      } else {

        Future.exception(new IllegalArgumentException("You don't know the secret"))
      }
    }
  }

  /**
   * The service simply echos back "hello world"
   */
  class Respond extends Service[Request, Response] {

    def apply(request: Request) = {

      val response = Response(Version.Http11, Status.Ok)
      response.contentString = "hello world"
      Future.value(response)
    }
  }

  def main(args: Array[String]) {

    val handleExceptions = new HandleExceptions
    val authorize = new Authorize
    val respond = new Respond

    /* Compose the Filters and Service together: */
    val myService: Service[Request, Response] = handleExceptions andThen authorize andThen respond

    val server: Server = ServerBuilder().codec(Http()).bindTo(new InetSocketAddress(8080)).name("httpserver").build(myService)
  }
}