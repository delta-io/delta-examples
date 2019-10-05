package mrpowers.delta.examples

import org.scalatest.FunSpec

class UpdateTableDataSpec extends FunSpec {

  it("updates a Delta table") {
    UpdateTableData.updateDeltaTable()
  }

  it("shows the updated table") {
    CreateATable.readTable()
  }

}
