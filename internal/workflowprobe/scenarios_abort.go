package workflowprobe

import (
	"fmt"
	"net/http"
	"sync"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runPlanAbort() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-abort-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-abort-02", "east"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "abort.example", 5001, 2, 1, 80)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "abort-lease-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 2 {
		return ensure(false, "expected two leases before abort")
	}
	if _, err := submitProbe(harness, group.LeaseIDs[0], "abort-probe-0001", true, revision.Serial, 10); err != nil {
		return err
	}

	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "abort-assess-0001",
	}, &assessment); err != nil {
		return err
	}

	var fact domain.PlanAbort
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/abort", domain.PlanAbortInput{
		Reason:      "wrong serial observed in canary region",
		RequestedBy: "rollout-operator",
	}, &fact); err != nil {
		return err
	}
	if fact.ID == "" || fact.PlanID != plan.ID || fact.AbortedAt.IsZero() {
		return ensure(false, "abort fact was not recorded")
	}
	if fact.Reason != "wrong serial observed in canary region" {
		return ensure(false, "abort reason was not preserved")
	}
	if fact.RevokedLeaseCount != 1 || len(fact.RevokedLeaseIDs) != 1 || fact.RevokedLeaseIDs[0] != group.LeaseIDs[1] {
		return ensure(false, "only the unfinished lease must be revoked")
	}
	if fact.PreservedProbes != 1 || len(fact.PreservedProbeIDs) != 1 {
		return ensure(false, "reported probes must be preserved on the abort fact")
	}
	if fact.PreservedAssessmentCount != 1 {
		return ensure(false, "stored assessments must be preserved on the abort fact")
	}

	var aborted domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &aborted); err != nil {
		return err
	}
	if aborted.State != domain.PlanAborted || aborted.AbortID != fact.ID || aborted.AbortedAt == nil {
		return ensure(false, "plan did not enter the terminal aborted state")
	}
	if aborted.CompletedAt != nil || aborted.ActivatedAt == nil {
		return ensure(false, "abort must not fabricate completion timestamps")
	}

	var fetched domain.PlanAbort
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/abort", nil, &fetched); err != nil {
		return err
	}
	if fetched.ID != fact.ID {
		return ensure(false, "abort fact lookup returned another fact")
	}

	var revoked domain.NodeLease
	if err := harness.call(http.MethodGet, "/v1/leases/"+group.LeaseIDs[1], nil, &revoked); err != nil {
		return err
	}
	if revoked.State != domain.LeaseRevoked || revoked.FinishedAt == nil {
		return ensure(false, "claimed lease was not revoked by abort")
	}
	var reported domain.NodeLease
	if err := harness.call(http.MethodGet, "/v1/leases/"+group.LeaseIDs[0], nil, &reported); err != nil {
		return err
	}
	if reported.State != domain.LeaseReported || reported.ProbeID == "" {
		return ensure(false, "reported lease must survive abort untouched")
	}

	var leasedNodes domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=leased", nil, &leasedNodes); err != nil {
		return err
	}
	if len(leasedNodes.Items) != 0 {
		return ensure(false, "revoked leases must release their nodes")
	}
	var readyNodes domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=ready", nil, &readyNodes); err != nil {
		return err
	}
	if len(readyNodes.Items) != 2 {
		return ensure(false, "both nodes must be ready after abort")
	}

	// Repeating the abort always converges on the original fact, even when the
	// retried request carries a different reason.
	var replay domain.PlanAbort
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/abort", domain.PlanAbortInput{
		Reason:      "a different reason from a retry",
		RequestedBy: "someone-else",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != fact.ID || replay.Reason != fact.Reason {
		return ensure(false, "repeated abort did not return the same fact")
	}

	// Terminal state: no further lifecycle move is possible.
	if err := mustFail(harness, http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": aborted.Version,
	}, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	if err := mustFail(harness, http.MethodPatch, "/v1/plans/"+plan.ID+"/activate", map[string]any{
		"expected_version": aborted.Version,
	}, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	if err := mustFail(harness, http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5002,
		Authority: "ns1.abort.example",
		Entries: []domain.ZoneEntry{
			{Owner: "abort.example", Type: domain.EntryA, Value: "192.0.2.20"},
		},
	}, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// New allocations stop; the original request key still replays its group.
	if _, err := allocate(harness, plan.ID, "abort-lease-0002", "east", 1); err != nil {
		if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
			return err
		}
	} else {
		return ensure(false, "allocation after abort must fail")
	}
	var replayedGroup domain.LeaseGroup
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/leases", domain.LeaseInput{
		RequestKey:   "abort-lease-0001",
		Region:       "east",
		LeaseSeconds: 600,
		MaxNodes:     2,
	}, &replayedGroup); err != nil {
		return err
	}
	if replayedGroup.ID != group.ID {
		return ensure(false, "lease replay after abort returned another group")
	}

	// Probes can no longer land on revoked leases, and the reported probe is
	// still readable through the lease link.
	if _, err := submitProbe(harness, group.LeaseIDs[1], "abort-probe-0002", true, revision.Serial, 10); err != nil {
		if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
			return err
		}
	} else {
		return ensure(false, "probe on revoked lease must fail")
	}
	var stillRevoked domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[1]+"/release", nil, &stillRevoked); err != nil {
		return err
	}
	if stillRevoked.State != domain.LeaseRevoked {
		return ensure(false, "release must not convert a revoked lease into released")
	}

	// Assessments stop but the stored verdict stays available.
	if err := mustFail(harness, http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "abort-assess-0002",
	}, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != assessment.ID {
		return ensure(false, "stored assessment must remain readable after abort")
	}

	// Empty reason is rejected without changing state.
	activePlan, _, err := createActivePlan(harness, "active-abort.example", 5101, 1, 1, 80)
	if err != nil {
		return err
	}
	if err := mustFail(harness, http.MethodPatch, "/v1/plans/"+activePlan.ID+"/abort", domain.PlanAbortInput{
		Reason: "   ",
	}, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	var stillActive domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+activePlan.ID, nil, &stillActive); err != nil {
		return err
	}
	if stillActive.State != domain.PlanActive {
		return ensure(false, "invalid abort must leave the plan active")
	}

	// Probe submission and abort compete on one claimed lease: the lease ends
	// up either reported (with one probe) or revoked (with none), never both.
	if _, err := enrollNode(harness, "resolver-abort-04", "north"); err != nil {
		return err
	}
	probeRacePlan, probeRaceRevision, err := createActivePlan(harness, "probe-race.example", 5301, 1, 1, 80)
	if err != nil {
		return err
	}
	probeRaceGroup, err := allocate(harness, probeRacePlan.ID, "probe-race-lease-0001", "north", 1)
	if err != nil {
		return err
	}
	probeRaceLeaseID := probeRaceGroup.LeaseIDs[0]
	const probeRacers = 16
	var probeWG sync.WaitGroup
	probeWG.Add(probeRacers)
	probeSuccesses := make(chan bool, probeRacers)
	for i := 0; i < probeRacers; i++ {
		i := i
		go func() {
			defer probeWG.Done()
			if i == 0 {
				err := harness.call(http.MethodPatch, "/v1/plans/"+probeRacePlan.ID+"/abort", domain.PlanAbortInput{
					Reason: "abort racing probe submissions",
				}, nil)
				probeSuccesses <- false
				_ = err
				return
			}
			_, err := submitProbe(
				harness,
				probeRaceLeaseID,
				fmt.Sprintf("probe-race-request-%04d", i),
				true,
				probeRaceRevision.Serial,
				10,
			)
			probeSuccesses <- err == nil
		}()
	}
	probeWG.Wait()
	close(probeSuccesses)
	submitted := 0
	for ok := range probeSuccesses {
		if ok {
			submitted++
		}
	}
	var settledLease domain.NodeLease
	if err := harness.call(http.MethodGet, "/v1/leases/"+probeRaceLeaseID, nil, &settledLease); err != nil {
		return err
	}
	var settledPlan domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+probeRacePlan.ID, nil, &settledPlan); err != nil {
		return err
	}
	switch {
	case settledLease.State == domain.LeaseRevoked:
		// Abort committed before any probe: the plan is terminal and the
		// claimed lease was revoked with no probe attached to it.
		if settledPlan.State != domain.PlanAborted || submitted != 0 {
			return ensure(false, fmt.Sprintf("revoked lease requires aborted plan and zero probes, got plan=%s probes=%d", settledPlan.State, submitted))
		}
	case settledLease.State == domain.LeaseReported:
		// One probe committed first (abort then preserves it); the plan is
		// either still active or aborted by the trailing abort call.
		if submitted != 1 {
			return ensure(false, fmt.Sprintf("reported lease must have exactly one probe, got %d", submitted))
		}
		if settledPlan.State != domain.PlanActive && settledPlan.State != domain.PlanAborted {
			return ensure(false, "reported-lease race must settle active or aborted")
		}
	default:
		return ensure(false, fmt.Sprintf("probe/abort race settled inconsistently: plan=%s lease=%s", settledPlan.State, settledLease.State))
	}
	var settledNodes domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=leased", nil, &settledNodes); err != nil {
		return err
	}
	if len(settledNodes.Items) != 0 {
		return ensure(false, "node must be released regardless of race outcome")
	}

	// Completion and abort compete on the same active version: exactly one
	// transition wins, and every abort response converges on one fact.
	if _, err := enrollNode(harness, "resolver-abort-03", "west"); err != nil {
		return err
	}
	racePlan, _, err := createActivePlan(harness, "race.example", 5201, 1, 1, 80)
	if err != nil {
		return err
	}
	if _, err := allocate(harness, racePlan.ID, "race-lease-0001", "west", 1); err != nil {
		return err
	}
	const competitors = 12
	type raceResult struct {
		kind    string
		success bool
		factID  string
	}
	results := make([]raceResult, competitors)
	var wg sync.WaitGroup
	wg.Add(competitors)
	for i := 0; i < competitors; i++ {
		i := i
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				err := harness.call(http.MethodPatch, "/v1/plans/"+racePlan.ID+"/complete", map[string]any{
					"expected_version": racePlan.Version,
				}, nil)
				results[i] = raceResult{kind: "complete", success: err == nil}
				return
			}
			var fact domain.PlanAbort
			if err := harness.call(http.MethodPatch, "/v1/plans/"+racePlan.ID+"/abort", domain.PlanAbortInput{
				Reason: fmt.Sprintf("concurrent abort attempt %d", i),
			}, &fact); err == nil {
				results[i] = raceResult{kind: "abort", success: true, factID: fact.ID}
			} else {
				results[i] = raceResult{kind: "abort", success: false}
			}
		}()
	}
	wg.Wait()

	var finished domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+racePlan.ID, nil, &finished); err != nil {
		return err
	}
	completeWins := 0
	abortWins := 0
	abortFactIDs := make(map[string]struct{})
	for _, result := range results {
		switch result.kind {
		case "complete":
			if result.success {
				completeWins++
			}
		case "abort":
			if result.success {
				abortWins++
				abortFactIDs[result.factID] = struct{}{}
			}
		}
	}
	switch finished.State {
	case domain.PlanCompleted:
		if completeWins != 1 || abortWins != 0 {
			return ensure(false, fmt.Sprintf("completed race must have one complete winner, got %d complete and %d abort", completeWins, abortWins))
		}
	case domain.PlanAborted:
		if completeWins != 0 || len(abortFactIDs) != 1 {
			return ensure(false, fmt.Sprintf("aborted race must converge on one fact, got %d complete and %d facts", completeWins, len(abortFactIDs)))
		}
	default:
		return ensure(false, "race plan must settle in a terminal state")
	}

	if finished.State == domain.PlanAborted {
		var raceFact domain.PlanAbort
		if err := harness.call(http.MethodPatch, "/v1/plans/"+racePlan.ID+"/abort", domain.PlanAbortInput{
			Reason: "concurrent abort attempt replay",
		}, &raceFact); err != nil {
			return err
		}
		if raceFact.ID != finished.AbortID {
			return ensure(false, "abort replay after race returned another fact")
		}
	} else if err := mustFail(harness, http.MethodPatch, "/v1/plans/"+racePlan.ID+"/abort", domain.PlanAbortInput{
		Reason: "abort after completion",
	}, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	return nil
}
