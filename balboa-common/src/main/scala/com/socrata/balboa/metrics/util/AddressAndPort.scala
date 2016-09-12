package com.socrata.balboa.metrics.util

import java.net.InetAddress

/**
 * @param address The address to connect to.
 * @param port Integer representation of port. IE. 8080, 9062
 */
case class AddressAndPort(address: InetAddress, port: Int) {

  override def toString: String = address.getHostAddress + ":" + port

}

object AddressAndPort {
  /**
   * Parses a comma separated list of host:port,host:port,host:port combinations into an actual list of Address and
   * Port objects.
   *
   * @param commaSeparatedList Comma separated list of (host:port) addresses
   * @return Structured list of [[AddressAndPort]]
   */
  def parse(commaSeparatedList: String): List[AddressAndPort] = commaSeparatedList match {
    case s: String =>
      commaSeparatedList.split(',')
        .map((aap: String) => aap.split(':'))
        .filter(a => a.length == 2)
        .map((aapa: Array[String]) => AddressAndPort(InetAddress.getByName(aapa(0)), aapa(1).toInt)).toList
    case _ => List.empty
  }
}
