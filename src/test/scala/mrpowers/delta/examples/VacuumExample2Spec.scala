package mrpowers.delta.examples

import org.scalatest.FunSpec

class VacuumExample2Spec extends FunSpec {

  it("creates the dog data store") {
    VacuumExample2.createDogStore()
  }

  it("displays the dog store") {
    VacuumExample2.displayDogStore()
  }

  it("vacuums the dog store") {
    VacuumExample2.vacuumDogStore()
  }

}
