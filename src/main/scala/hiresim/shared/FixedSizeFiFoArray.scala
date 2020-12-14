package hiresim.shared

abstract class FixedSizeFiFoArray[N >: Null](maxSize: Int) {

  protected val all: Array[N] = getArray(maxSize)
  private var nextWrite: Int = 0
  private var allValid: Boolean = false

  def size: Int = if (allValid) all.length else nextWrite

  def isEmpty: Boolean = !allValid && nextWrite == 0

  protected def getValidArrayBound: Int = if (allValid) all.length else nextWrite

  def getPreviousItem: Option[N] =
    if (nextWrite > 0) Some(all(nextWrite - 1))
    else if (allValid) Some(all.last)
    else None

  def getOldestItem: Option[N] =
    if (allValid) Some(all(nextWrite))
    else if (nextWrite > 0) Some(all(0))
    else None

  @inline protected def pushPointer: Unit = {
    nextWrite += 1
    if (nextWrite == all.length) {
      nextWrite = 0
      allValid = true
    }
  }

  def enqueue(item: N): Unit = {
    all(nextWrite) = item
    pushPointer
  }

  protected def getArray(size: Int): Array[N]

  protected def getUnsortedValidArraySlice: Array[N] = if (allValid) all else all.slice(0, nextWrite)
}
