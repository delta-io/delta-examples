package mrpowers.delta.examples

import org.scalatest.FunSpec

class ConditionalUpdateWithoutOverwriteSpec extends FunSpec {

  it("creates an initial dataset") {
    ConditionalUpdateWithoutOverwrite.createInitialDataset()
  }

  it("adds one hundred to even numbers") {
    ConditionalUpdateWithoutOverwrite.addOneHundredToEvens()
  }

  it("deletes the even numbers") {
    ConditionalUpdateWithoutOverwrite.deleteEvenNumbers()
  }

  it("upserts new data") {
    ConditionalUpdateWithoutOverwrite.upsertNewData()
  }

}
