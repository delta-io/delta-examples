package mrpowers.delta.examples

import org.scalatest.FunSpec

class IncrementalDogUpdaterSpec extends FunSpec {

  it("updates a lake with Structured Streaming + Trigger.Once") {
    IncrementalDogUpdater.update()
  }

}
