import java.io.{BufferedWriter, FileReader, FileWriter, IOException, RandomAccessFile}
import java.nio.{BufferUnderflowException, ByteBuffer}
import java.nio.channels.FileChannel
import java.nio.file.{FileAlreadyExistsException, FileSystemException, Files, Path, Paths}
import java.util.Scanner
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.StdIn.readLine

/** ByteParser reads in a file and writes its extension, size, and non-zero bit locations into a .byteparser file.
 *  For .byteparser files, it creates a RandomAccessFile named the same as the .byteparser's filename with the
 *  original extension, resizes it to the original file size, and then writes its bytes to the appropriate
 *  positions to recreate the original file.
 */
object ByteParser {

  var fileSize : Long = 0
  val atomicBytesWritten : AtomicLong = new AtomicLong(0)
  val writerMap : TrieMap[Byte, BufferedWriter] = TrieMap()
  var workers : Array[Future[Boolean]] = null

  def reconstruct(file : String) : Unit = {
    try {
      var first : Boolean = true
      var second : Boolean = true
      val scanner : Scanner = new Scanner(Paths.get(file).toFile)
      scanner.useDelimiter("[,:]")
      var channel : FileChannel = null
      var newFile : RandomAccessFile = null
      var totalLength : Long = 0
      var currentLength : Long = 0
      var byteVal : Byte = 0

      while(scanner.hasNext) {
        if (first) {
          val ext : String = scanner.nextLine()

          if (Files.exists(Paths.get(Paths.get(file).getParent.toString, Paths.get(file).getFileName.toString.replace("byteparser", ext)))) {
            println(s"Unable to recreate file, \"${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace("byteparser", ext)}\" exists.")
            println("Please delete or relocate and try again.")
            readLine()
            System.exit(-1)
          }

          newFile = new RandomAccessFile(
            Files.createFile(
              Paths.get(
                Paths.get(file).getParent.toString,
                Paths.get(file).getFileName.toString.replace("byteparser", ext)
              )
            ).toFile
            ,"rw"
          )
          first = false
          println(s"Created destination file: \"${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace("byteparser", ext)}\".")
        } else if (second) {
          totalLength = scanner.nextLine.toLong
          newFile.setLength(totalLength)
          channel = newFile.getChannel
          second = false
          println(s"Expanded file to $totalLength bytes and opened FileChannel for writing.")
        } else {
          if (byteVal == 0)
            byteVal = scanner.nextByte()
          val arr: Array[Byte] = Array(0)
          var num : String = ""
          var longNum : Long = 0
          arr.update(0, byteVal)

          do {
            num = scanner.next()

            if (num.contains("\n")) {
              longNum = num.split("\n")(0).toLong
              if (num.split("\n").length > 1)
                byteVal = num.split("\n")(1).toByte
            } else
              longNum = num.replace(",","").toLong

            channel.position(longNum).write(ByteBuffer.wrap(arr))
            currentLength += 1
            print(s"\rWrote $currentLength of $totalLength bytes.")
          } while (!num.contains("\n"))
        }
      }

      print(s"\rWrote $totalLength of $totalLength bytes.")
      println()
      println("Finished regenerating file! Closing streams.")
      channel.close()
      newFile.close()
      scanner.close()
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
      println(s"Unable to parse file, .byteparser file already exists (\'${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace(
        "." + Paths.get(file).getFileName.toString.split("\\.").last,
        ".byteparser"
      )}.")
      println("Please delete and try again.")
      readLine()
      System.exit(-1)
    }

    try {
      val outputFile : Path = Files.createFile(
        Paths.get(
          Paths.get(file).getParent.toString,
          Paths.get(file).getFileName.toString.replace(
            "." + Paths.get(file).getFileName.toString.split("\\.").last,
            ".byteparser"
          )
        )
      )
      println(s"Created .byteparser file: \"${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace("." + Paths.get(file).getFileName.toString.split("\\.").last,".byteparser")}\".")

      val writer : FileWriter = new FileWriter(outputFile.toFile)
      writer.write(Paths.get(file).getFileName.toString.split("\\.").last + "\n")
      writer.flush()
      println(s"Wrote extension to file: ${Paths.get(file).getFileName.toString.split("\\.").last}")

      fileSize = Files.size(Paths.get(file))
      writer.write(fileSize + "\n")
      writer.flush()
      println(s"Wrote file size to file: $fileSize")

      val coreCount : Int = Runtime.getRuntime.availableProcessors()
      val threadByteCounts : mutable.ListBuffer[Long] = mutable.ListBuffer()
      for (i : Int <- 0 until coreCount)
        threadByteCounts.insert(i, Math.floor(fileSize / coreCount).toLong)

      if ((fileSize - (Math.floor(fileSize / coreCount).toLong * coreCount)) > 0)
        for (i : Int <- 0 until (fileSize - (Math.floor(fileSize / coreCount).toLong * coreCount)).toInt)
          threadByteCounts(i) += 1

      val randFile : RandomAccessFile = new RandomAccessFile(Paths.get(file).toFile, "r")
      var pos : Long = 0
      workers = Array.ofDim(coreCount)
      for (i : Int <- 0 until coreCount) {
        workers(i) = parallelParse(randFile.getChannel, pos, threadByteCounts(i), Paths.get(file).getParent.toString)
        pos += threadByteCounts(i)
      }
      do {
        print(s"\rWrote ${atomicBytesWritten.get()} of $fileSize Bytes.")
        Thread.sleep(1000)
      } while(!atomicBytesWritten.get().equals(fileSize))

      print(s"\rWrote ${atomicBytesWritten.get()} of $fileSize Bytes.")
      writerMap.values.foreach(p => p.close())
      writerMap.clear()
      randFile.close()

      println()
      println("Combining files...")
      for(i : Int <- -128 to 127)
        if (Files.exists(Paths.get(Paths.get(file).getParent.toString, s"\\$i.txt"))) {
          // Replacing last comma with a newline.
          val rnd : RandomAccessFile = new RandomAccessFile(Paths.get(Paths.get(file).getParent.toString + s"\\$i.txt").toFile, "rw")
          rnd.seek(rnd.length()-1)
          rnd.write(10)
          rnd.close()

          val fr : FileReader = new FileReader(Paths.get(file).getParent.toString + s"\\$i.txt")
          writer.write(s"$i:")
          fr.transferTo(writer)
          writer.flush()
          fr.close()
        }

      println(s"Finished writing .byteparser file \"${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace("." + Paths.get(file).getFileName.toString.split("\\.").last,".byteparser")}\"!")
      writer.close()
    } catch {
      case faee: FileAlreadyExistsException => println(
        s"Error while creating .byteparser file for \"$file\": File already exists!" +
          System.lineSeparator() + faee
      )
      case se : SecurityException => println(s"Error while attempting to create .byteparser file for \"$file\": $se")
    } finally {
      println("Cleaning up temporary files...")
      for(i : Int <- -128 to 127) {
        if (i != 0) {
          var deleted: Boolean = false
          do {
            try {
              deleted = Files.deleteIfExists(Paths.get(Paths.get(file).getParent.toString, s"\\$i.txt"))
            } catch {
              case fse: FileSystemException =>
                println(Paths.get(file).getParent.toString + s"\\$i.txt is still in use! Waiting three seconds before retrying...")
                System.gc()
                Thread.sleep(3000)
            }
          } while (!deleted)
        }
      }
    }
  }

  private def parallelParse(channel : FileChannel, start : Long, len : Long, parentPath : String) : Future[Boolean] = {
    Future[Boolean] {
      try {
      var pos : Long = start
      val max : Long = start + len
      var buffer : ByteBuffer = null
      var arr : Array[Byte] = null

      if ((max - pos) > Integer.MAX_VALUE) {
        buffer = ByteBuffer.allocateDirect(Integer.MAX_VALUE)
        arr = Array.ofDim[Byte](buffer.capacity())
      }

      while (pos < max) {
        if (max - pos < Integer.MAX_VALUE) {
          buffer = ByteBuffer.allocateDirect((max - pos).toInt)
          arr = Array.ofDim[Byte](buffer.capacity())
        }
        channel.position(pos)
        channel.read(buffer)
        buffer.flip()
        buffer.get(arr)
        arr.indices.foreach(b => {
          if (arr(b) != 0) {
            if (!writerMap.contains(arr(b))) {
              val writer : BufferedWriter = new BufferedWriter(new FileWriter(Paths.get(parentPath + s"\\${arr(b)}.txt").toFile, true))
              //threadWriters.append(writer)
              writerMap.putIfAbsent(arr(b), writer)
            }
            writerMap(arr(b)).write((pos + b).toString + ",")
          }
          atomicBytesWritten.incrementAndGet()
        })
        pos += buffer.capacity()
      }
      buffer.clear()
      true
    } catch {
        case bue : BufferUnderflowException =>
          println("Received BufferUnderflowException!")
          false
      }
    }
  }
}
