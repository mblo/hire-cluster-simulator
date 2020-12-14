package hiresim.shared

import hiresim.simulation.configuration.SimulationConfiguration

object Logging {

  final val PrefixValidation: String = "[Validate]"
  final val PrefixVerbose: String = "[Verbose]"
  final val PrefixVerboseCell: String = "[Verbose/Cell]"

  def validation(message: => String): Unit = {
    if (SimulationConfiguration.LOGGING_VERBOSE_OTHER) {
      print(PrefixValidation, message)
    }
  }

  def verbose(message: => String): Unit = {
    if (SimulationConfiguration.LOGGING_VERBOSE_OTHER) {
      println(PrefixVerbose + message)
    }
  }

  def verbose_cell(message: => String,
                   ident: Boolean = true,
                   break: Boolean = true): Unit = {
    if (SimulationConfiguration.LOGGING_VERBOSE_CELL) {
      print(PrefixVerboseCell, message, ident = ident, break = break)
    }
  }

  private def print(prefix: String,
                    message: => String,
                    ident: Boolean = true,
                    break: Boolean = true): Unit = {

    val output: String =
      if (ident)
        prefix + " " + message
      else
        message

    if (break) {
      Console.out.println(output)
    } else {
      Console.out.print(output)
      Console.out.flush()
    }

  }

}
