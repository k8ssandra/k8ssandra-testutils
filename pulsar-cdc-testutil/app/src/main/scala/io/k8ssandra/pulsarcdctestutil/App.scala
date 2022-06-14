package io.k8ssandra.pulsarcdctestutil

import picocli.CommandLine

object App {
  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new RunTests()).execute(args: _*))
  }
}