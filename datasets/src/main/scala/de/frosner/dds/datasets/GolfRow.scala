package de.frosner.dds.datasets

case class GolfRow(outlook: String,
                   temperature: Double,
                   humidity: Double,
                   wind: Boolean,
                   play: Boolean) {
  // override equals because default implementation using pattern matching does not work in Spark REPL
  override def equals(thatAny: Any): Boolean = {
    if (thatAny.isInstanceOf[GolfRow]) {
      val that = thatAny.asInstanceOf[GolfRow]
      this.outlook == that.outlook &&
        this.temperature == that.temperature &&
        this.humidity == that.humidity &&
        this.wind == that.wind &&
        this.play == that.play
    } else {
      false
    }
  }
}
