(ns elephantdb.common.status-test
  (:use elephantdb.common.status
        midje.sweet)
  (:import elephantdb.common.status.KeywordStatus))

(let [status (atom (KeywordStatus. :loading))]
  "Stateful transition testing. We start by making sure we've got a
   proper loading atom: "
  (fact @status => loading?)

  "Now we swap over to ready and make sure that the status reflects
   this."
  (swap-status! status to-ready)
  (fact @status => ready?)

  "Back to loading should give us the original two, plus updating?"
  (swap-status! status to-loading)
  (fact @status => (every-pred loading? updating?))

  "Failure should replace knock out all statuses but failed."
  (swap-status! status to-failed "Testing failure.")
  (facts
    @status =not=> (some-fn loading? ready? updating?)
    @status => failed?))
