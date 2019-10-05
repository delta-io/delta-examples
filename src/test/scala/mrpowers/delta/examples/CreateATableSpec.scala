package mrpowers.delta.examples

import org.scalatest.FunSpec

class CreateATableSpec extends FunSpec {

  it("creates a Delta table") {
    CreateATable.createTable()
  }

  it("displays a Delta table") {
    CreateATable.readTable()
  }

}
