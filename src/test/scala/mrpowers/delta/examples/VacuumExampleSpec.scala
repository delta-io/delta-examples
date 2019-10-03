package mrpowers.delta.examples

import org.scalatest.FunSpec

class VacuumExampleSpec extends FunSpec {

  it("creates an initial data store") {
    VacuumExample.createInitialPeopleStore()
  }

  it("displays the data store 1") {
    VacuumExample.displayPeopleStorev0()
  }

  it("overwrites the data store") {
    VacuumExample.overwritePeopleStore()
  }

  it("displays the data store 2") {
    VacuumExample.displayPeopleStorev0()
  }

  it("vacuums the data store") {
    VacuumExample.vacuumPeopleStore()
  }

  it("displays the data store 3") {
    VacuumExample.displayPeopleStorev0()
  }


}
