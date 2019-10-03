package mrpowers.delta.examples

import org.scalatest.FunSpec

class TaxiLakeCreatorSpec extends FunSpec {

  it("creates a parquet lake") {
    TaxiLakeCreator.createParquetLake()
  }

  it("creates a delta lake") {
    TaxiLakeCreator.createDeltaLake()
  }

  it("incrementally updates a delta lake") {
    TaxiLakeCreator.createIncrementalDeltaLake()
  }

}
