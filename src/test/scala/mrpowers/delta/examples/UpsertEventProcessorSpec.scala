package mrpowers.delta.examples

import org.scalatest.FunSpec

class UpsertEventProcessorSpec extends FunSpec {

//  it("creates a delta lake") {
//    UpsertEventProcessor.createUpsertEventsDeltaLake()
//  }
//
//  it("shows the initial state of the lake") {
//    UpsertEventProcessor.displayEvents()
//  }
//
//  it("performs the upsert") {
//    UpsertEventProcessor.doTheUpsert()
//  }
//
//  it("shows the post upsert state of the lake") {
//    UpsertEventProcessor.displayEvents()
//  }

  it("shows part-00000-36aafda3-530d-4bd7-a29b-9c1716f18389-c000") {
    UpsertEventProcessor.displayEventParquetFile("part-00000-36aafda3-530d-4bd7-a29b-9c1716f18389-c000")
  }

  it("shows part-00026-fcb37eb4-165f-4402-beb3-82d3d56bfe0c-c000") {
    UpsertEventProcessor.displayEventParquetFile("part-00026-fcb37eb4-165f-4402-beb3-82d3d56bfe0c-c000")
  }

  it("shows part-00139-eab3854f-4ed4-4856-8268-c89f0efe977c-c000") {
    UpsertEventProcessor.displayEventParquetFile("part-00139-eab3854f-4ed4-4856-8268-c89f0efe977c-c000")
  }

  it("shows part-00166-0e9cddc8-9104-4c11-8b7f-44a6441a95fb-c000") {
    UpsertEventProcessor.displayEventParquetFile("part-00166-0e9cddc8-9104-4c11-8b7f-44a6441a95fb-c000")
  }

  it("shows part-00178-147c78fa-dad2-4a1c-a4c5-65a1a647a41e-c000") {
    UpsertEventProcessor.displayEventParquetFile("part-00178-147c78fa-dad2-4a1c-a4c5-65a1a647a41e-c000")
  }

}
