import java.io.File
import java.nio.file.{Path, Paths}
import scala.io.StdIn.readLine

object Main {
  def main(args : Array[String]) : Unit = {
    if (args.length == 0) {
      println("Please drop a non .byteparser file on to the jar or pass it in as an argument!")
      print("Press enter to continue...")
      readLine()
      return
    }

    for (filePath : String <- args) {
      def path : Path = Paths.get(filePath)

      def file : File = path.toFile
      if (file.isFile)
        if (path.getFileName.toString.split("\\.").last.equals("byteparser"))
          ByteParser.reconstruct(filePath)
        else
          ByteParser.parse(filePath)
      else
        println(s"Unable to parse directory or unknown \"$filePath\" at this time!")
    }
  }
}
