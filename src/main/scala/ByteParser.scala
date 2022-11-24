import java.io._
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import scala.collection.mutable
import scala.io.StdIn.readLine

object ByteParser {
  def reconstruct(file : String) : Unit = {
    try {
      var first : Boolean = true
      var second : Boolean = true
      val reader : BufferedReader = Files.newBufferedReader(Paths.get(file))
      var channel : FileChannel = null
      var newFile : RandomAccessFile = null
      var totalLength : Long = 0
      var currentLength : Long = 0



      reader.lines().forEachOrdered(line =>
      {
        if (line.nonEmpty)
          if (first) {
            newFile = new RandomAccessFile(
              Files.createFile(
                Paths.get(
                  Paths.get(file).getParent.toString,
                  Paths.get(file).getFileName.toString.replace("byteparser", line)
                )
              ).toFile
              ,"w"
            )
            first = false
            println(s"Created destination file: \"${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace("byteparser", line)}\".")
          } else if (second) {
            totalLength = new BigInteger(line).longValueExact()
            newFile.setLength(totalLength)
            channel = newFile.getChannel
            second = false
            println(s"Expanded file to $line bytes and opened FileChannel for writing.")
          } else {
            var byteVal : Byte = 0
            var firstNum : Boolean = true
            val arr : Array[Byte] = Array.emptyByteArray
            for (stringNum : String <- line.split("[,:\\n]")) {
              if (firstNum) {
                byteVal = stringNum.toByte
                firstNum = false
              } else {
                arr.update(0, byteVal)
                channel.position(stringNum.toLong).write(ByteBuffer.wrap(arr))
                currentLength+=1
                print(s"\rWrote $currentLength of $totalLength bytes.")
              }
            }
          }
      })

      println()
      println("Finished regenerating file! Closing streams.")
      channel.close()
      newFile.close()
      reader.close()
    } catch {
      case io : IOException => println(s"Error while reading file \"$file\": $io")
      case ae : ArithmeticException => println(
        s"Error while reading file \"$file\": File size is too large!" +
          System.lineSeparator() + ae
      )
    }
  }

  def parse(file : String) : Unit = {
    for(i : Int <- -128 to 127)
      if (Files.exists(Paths.get(Paths.get(file).getParent.toString, s"\\$i.txt"))) {
        println(s"Unable to parse file, \"${Paths.get(file).getParent.toString}\\$i.txt\" exists.")
        println("Please delete or relocate and try again.")
        readLine()
        System.exit(-1)
      }

    if (Files.exists(Paths.get(
      Paths.get(file).getParent.toString,
      Paths.get(file).getFileName.toString.replace(
        "." + Paths.get(file).getFileName.toString.split("\\.").last,
        ".byteparser"
      )
    ))) {
      println(s"Unable to parse file, byteparser file already exists (\'${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace(
        "." + Paths.get(file).getFileName.toString.split("\\.").last,
        ".byteparser"
      )}.")
      println("Please delete and try again.")
      readLine()
      System.exit(-1)
    }

    try {
      var fileSize : Long = 0
      var currentByte : Long = 0

      val outputFile : Path = Files.createFile(
        Paths.get(
          Paths.get(file).getParent.toString,
          Paths.get(file).getFileName.toString.replace(
            "." + Paths.get(file).getFileName.toString.split("\\.").last,
            ".byteparser"
          )
        )
      )
      println(s"Created byteparser file: \"${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace("." + Paths.get(file).getFileName.toString.split("\\.").last,".byteparser")}\".")

      val writer : FileWriter = new FileWriter(outputFile.toFile)
      writer.write(Paths.get(file).getFileName.toString.split("\\.").last + "\n")
      writer.flush()
      println(s"Wrote extension to file: ${Paths.get(file).getFileName.toString.split("\\.").last}")

      fileSize = Files.size(Paths.get(file))
      writer.write(fileSize + "\n")
      writer.flush()
      println(s"Wrote file size to file: $fileSize")

      val first : mutable.Map[Byte, Boolean] = mutable.Map()
      for(i : Int <- -128 to 127)
        first.put(i.toByte, true)

      val bis : BufferedInputStream = new BufferedInputStream(new FileInputStream(file))
      Iterator.continually(bis.read())
        .takeWhile(_ != -1)
        .foreach(b => {
          if (b != 0)
            if(first(b.toByte)) {
              val fw : FileWriter = new FileWriter(Paths.get(file).getParent.toString + s"\\${b.toByte}.txt")
              fw.write(currentByte.toString)
              fw.flush()
              fw.close()
              first.put(b.toByte, false)
            } else {
              val fw : FileWriter = new FileWriter(Paths.get(file).getParent.toString + s"\\${b.toByte}.txt", true)
              fw.write(s",$currentByte")
              fw.flush()
              fw.close()
            }

          currentByte += 1
          print(s"\rWrote Byte $currentByte of $fileSize.")
        })
      bis.close()

      println()
      println("Combining files...")
      for(i : Int <- -128 to 127)
        if (Files.exists(Paths.get(Paths.get(file).getParent.toString, s"\\$i.txt"))) {
          println(s"Combining file \"${Paths.get(file).getParent.toString + s"\\$i.txt"}\"...")
          val fr : FileReader = new FileReader(Paths.get(file).getParent.toString + s"\\$i.txt")
          writer.write(s"$i:");
          fr.transferTo(writer)
          fr.close()
          writer.write("\n")
          writer.flush()
        }

      println(s"Finished writing byteparser file \"${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace("." + Paths.get(file).getFileName.toString.split("\\.").last,".byteparser")}\"!")
      writer.close()
    } catch {
      case faee: FileAlreadyExistsException => println(
        s"Error while creating byteparser file for \"$file\": File already exists!" +
          System.lineSeparator() + faee
      )
      case se : SecurityException => println(s"Error while attempting to create byteparser file for \"$file\": $se")
    } finally {
      println("Cleaning up temporary files...")
      for(i : Int <- -128 to 127)
        if (Files.deleteIfExists(Paths.get(Paths.get(file).getParent.toString, s"\\$i.txt")))
          println(s"Deleted \"${Paths.get(file).getParent.toString + s"\\$i.txt"}\".")
    }
  }
}
