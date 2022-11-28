import java.io.{BufferedWriter, FileReader, FileWriter, IOException, RandomAccessFile}
import java.nio.channels.FileChannel.MapMode
import java.nio.{BufferUnderflowException, ByteBuffer, MappedByteBuffer}
import java.nio.channels.{AsynchronousFileChannel, FileChannel, SeekableByteChannel}
import java.nio.file.{FileAlreadyExistsException, FileSystemException, Files, Path, Paths, StandardOpenOption}
import java.util.Scanner
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
  var workers : Array[Future[Boolean]] = _
  var allWorkersDone : Boolean = false
  val valueMap : scala.collection.concurrent.TrieMap[Long, Byte] = TrieMap()
  val oneOffs : scala.collection.mutable.ListBuffer[Long] = ListBuffer()

  def reconstruct(file : String) : Unit = {
    try {
      val scanner : Scanner = new Scanner(Paths.get(file).toFile)
      scanner.useDelimiter("[,:]")
      var newFile : RandomAccessFile = null
      var fileChannel : SeekableByteChannel = null


      val newlinePositions : ListBuffer[Long] = ListBuffer()
      val sbc : SeekableByteChannel = Files.newByteChannel(Paths.get(file))
      var pos : Long = 0
      var read : Int = 0
      val bb : ByteBuffer = ByteBuffer.allocateDirect(Integer.MAX_VALUE)
      do {
        read = sbc.read(bb)
        if (read > 0) {
          val arr : Array[Byte] = Array.ofDim[Byte](read)
          bb.flip()
          bb.get(arr)
          arr.indices.foreach(i =>
            if (arr(i) == 10)
              newlinePositions.append(pos + i)
          )
          pos += read
        }
      } while(read > 0)

      sbc.close()


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
      println(s"Created destination file: \"${Paths.get(file).getParent.toString}\\${Paths.get(file).getFileName.toString.replace("byteparser", ext)}\".")
      fileChannel = Files.newByteChannel(Paths.get(file), StandardOpenOption.READ)
      fileSize = scanner.nextLine.toLong
      newFile.setLength(fileSize)
      println(s"Expanded file to $fileSize bytes and opened FileChannel for writing.")
      scanner.close()

      workers = Array.ofDim(newlinePositions.length)
      for(i : Int <- 1 until newlinePositions.length - 1)
        workers.update(i-1, reconstructParallel(newlinePositions(i), newlinePositions(i+1)+1, newFile.getChannel, fileChannel))

      val queue : Future[Boolean] = processQueue(newFile.getChannel)

      do {
        print(s"\rWrote ${atomicBytesWritten.get()} of $fileSize bytes.")
        Thread.sleep(1000)
        allWorkersDone = workers.filter(w => w != null).forall(w => w.isCompleted)
      } while (!allWorkersDone || !queue.isCompleted)

      fileChannel.close()
      newFile.getChannel.close()

      print(s"\rWrote ${atomicBytesWritten.get()} of $fileSize bytes.")
      println()
      println("Finished regenerating file! Closing streams.")
    } catch {
      case io : IOException => println(s"Error while reading file \"$file\": $io")
      case ae : ArithmeticException => println(
        s"Error while reading file \"$file\": File size is too large!" +
          System.lineSeparator() + ae
      )
    }
  }

  private def processQueue(file : FileChannel) : Future[Boolean] = {
    Future[Boolean] {
      var pos : Long = 0
      var size : Long = 1024*1024*1024
      var mbb : MappedByteBuffer = null

      do {
        if (pos + size > fileSize)
          size = fileSize - pos
        mbb = file.map(MapMode.READ_WRITE, pos, size)
        mbb.load()
        while (pos < size) {
          if (valueMap.contains(pos) || oneOffs.contains(pos)) {
            mbb.put(valueMap.remove(pos).get)
            atomicBytesWritten.incrementAndGet()
            pos += 1
          } else if (!valueMap.contains(pos) && allWorkersDone) { // It is a zero, so skip.
            mbb.put(Byte.box(0))
            atomicBytesWritten.incrementAndGet()
            pos += 1
          } else if (!valueMap.contains(pos) && valueMap.size == Integer.MAX_VALUE) { // Buffer is full, process a one-off.
            val oneOffPos : Long = valueMap.keys.last
            oneOffs.append(oneOffPos)
            mbb.put(valueMap.remove(oneOffPos).get)
            atomicBytesWritten.incrementAndGet()
            pos += 1
          } else
            Thread.sleep(1000)
        }
        if (pos + size > fileSize)
          pos = fileSize - pos
        else
          pos += size
        mbb.force()
      } while (!allWorkersDone || valueMap.nonEmpty)
      true
    }
  }

  private def reconstructParallel(start : Long, end : Long, file : FileChannel, sbc : SeekableByteChannel) : Future[Boolean] = {
    Future[Boolean] {
      val byteVal : Array[Byte] = Array(0)
      var bb : ByteBuffer = null
      var pos : Long = start+1
      var read : Int = 0

      if (end - pos > Integer.MAX_VALUE)
        bb = ByteBuffer.allocateDirect(Integer.MAX_VALUE)

      var foundNewline : Boolean = false
      do {
        if (end - pos <= Integer.MAX_VALUE)
          bb = ByteBuffer.allocateDirect((end - pos).toInt)
        read = sbc.position(pos).read(bb)
        if (read > 0) {
          val arr: Array[Byte] = Array.ofDim[Byte](read)
          bb.flip()
          bb.get(arr)
          val num : mutable.StringBuilder = new mutable.StringBuilder()
          var i : Int = 0;
          while(i < arr.length) {
            if (arr(i) == ':') {
              byteVal.update(0, num.result.toByte)
              num.clear()
            } else if (arr(i) == ',') {
              if (valueMap.size == Integer.MAX_VALUE)
                Thread.sleep(1000)
              valueMap.put(num.result.toLong, byteVal(0))
              num.clear()
            } else if (arr(i) == 10) {
              foundNewline = true
              if (num.nonEmpty)
                valueMap.put(num.result.toLong, byteVal(0))
            } else
              num.append(Byte.box(arr(i)).toChar)
            i += 1
          }
          pos += read
        }
      } while (!foundNewline)
      true
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
