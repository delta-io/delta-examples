package mrpowers.delta.examples

import org.scalatest.FunSpec

class CatLakeCreatorSpec extends FunSpec {

    it("creates a cat data lake") {
      CatLakeCreator.createCatDeltaLake()
    }

    it("updates the fluffy misspelling") {
      CatLakeCreator.updateCatNameSpelling()
    }

}
