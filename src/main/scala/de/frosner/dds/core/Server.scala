package de.frosner.dds.core

/**
 * [[Server]] that connects the browser of the client with the back-end by serving [[Servable]]s. [[Servable]]s can
 * be tables, charts, etc. A server does not guarantee that it can be reused. So the user should invoke
 * start only one time and not try to restart the same server instance.
 */
trait Server {

  /**
   * Starts the server.
   */
  def start()

  /**
   * Stops the server.
   */
  def stop()

  /**
   * Serve the given [[Servable]] to the front-end. It will be sent to the browser with the next HTTP response.
   *
   * @param servable to serve to the front-end
   */
  def serve(servable: Servable)

}
