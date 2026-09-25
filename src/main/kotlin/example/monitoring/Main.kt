package example.monitoring

import kotlin.system.exitProcess

fun main(args: Array<String>) {
  // println("service mode: ${args.firstOrNull()}")
    val serviceArgs=args.drop(1).toTypedArray()

    when(args.firstOrNull()) {
        "central"-> startCentral(serviceArgs)
        "warehouse" ->startWarehouse(serviceArgs)
        else -> {
       // println("missing or unknown service mode")
            System.err.println("Usage: sensor-monitoring <central|warehouse> [--health-check]")
          exitProcess(2)
        }
    }
}
