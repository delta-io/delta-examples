package mrpowers.delta

import java.io.IOException
import java.nio.file.{Files, Paths, Path, SimpleFileVisitor, FileVisitResult}
import java.nio.file.attribute.BasicFileAttributes

object NioUtil {

  def remove(root: Path, deleteRoot: Boolean = true): Unit =
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exception: IOException): FileVisitResult = {
        if (deleteRoot) Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })

  def removeUnder(string: String): Unit = remove(Paths.get(string), deleteRoot=false)

  def removeAll(string: String): Unit = remove(Paths.get(string))

  def removeUnder(file: java.io.File): Unit = remove(file.toPath, deleteRoot=false)

  def removeAll(file: java.io.File): Unit = remove(file.toPath)

}
