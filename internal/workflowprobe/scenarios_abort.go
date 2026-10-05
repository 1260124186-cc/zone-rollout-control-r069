package workflowprobe

import (
	"fmt"
	"net/http"
	"reflect"
	"sync"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runPlanAbort() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	for _, name := range []string{"resolver-abort-01", "resolver-abort-02", "resolver-abort-03"} {
		if _, err := enrollNode(harness, name, "east"); err != nil {
			return err
		}
	}
	plan, revision, err := createActivePlan(harness, "abort.example", 5001, 2, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "abort-lease-0001", "east", 3)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 3 {
		return ensure(false, "abort setup expected three leases")
	}
	stored, err := submitProbe(harness, group.LeaseIDs[0], "abort-probe-0001", true, revision.Serial, 15)
	if err != nil {
		return err
	}
	var preAbort domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "abort-assess-0001",
	}, &preAbort); err != nil {
		return err
	}
	var beforeAbort domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &beforeAbort); err != nil {
		return err
	}

	// An abort carrying a stale expected version must not change anything.
	_, err = abortPlan(harness, plan.ID, domain.AbortInput{
		Reason:          "stale version attempt",
		ExpectedVersion: beforeAbort.Version + 9,
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}

	record, err := abortPlan(harness, plan.ID, domain.AbortInput{
		Reason:     "authority serving stale serial",
		Initiator:  "ops-oncall",
		RequestKey: "abort-request-0001",
	})
	if err != nil {
		return err
	}
	if record.ID == "" || record.PlanID != plan.ID {
		return ensure(false, "abort record does not reference the plan")
	}
	if record.Reason != "authority serving stale serial" || record.Initiator != "ops-oncall" {
		return ensure(false, "abort record lost reason or initiator")
	}
	wantRevoked := []string{group.LeaseIDs[1], group.LeaseIDs[2]}
	if wantRevoked[0] > wantRevoked[1] {
		wantRevoked[0], wantRevoked[1] = wantRevoked[1], wantRevoked[0]
	}
	if !reflect.DeepEqual(record.RevokedLeaseIDs, wantRevoked) {
		return ensure(false, "abort did not revoke exactly the unfinished leases")
	}
	if record.PreservedProbeCount != 1 {
		return ensure(false, "abort record did not count the preserved probe")
	}
	if record.AbortedAt.IsZero() {
		return ensure(false, "abort record is missing the abort timestamp")
	}

	var aborted domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &aborted); err != nil {
		return err
	}
	if aborted.State != domain.PlanAborted || aborted.AbortedAt == nil || aborted.AbortID != record.ID {
		return ensure(false, "plan aggregate does not reflect the abort")
	}
	if aborted.Version != beforeAbort.Version+1 {
		return ensure(false, "abort must advance the plan version exactly once")
	}
	if aborted.LatestAssessmentID != preAbort.ID {
		return ensure(false, "abort must preserve the latest assessment reference")
	}

	var fetched domain.PlanAbort
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/abort", nil, &fetched); err != nil {
		return err
	}
	if fetched.ID != record.ID || fetched.Reason != record.Reason {
		return ensure(false, "abort lookup returned a different fact")
	}

	// Nodes behind revoked leases are ready again; every east node is free.
	var readyPage domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=ready&region=east", nil, &readyPage); err != nil {
		return err
	}
	if len(readyPage.Items) != 3 {
		return ensure(false, "revoked leases did not return every node to ready")
	}

	// The reported lease keeps its probe: an identical replay still returns it.
	replay, err := submitProbe(harness, group.LeaseIDs[0], "abort-probe-0001", true, revision.Serial, 15)
	if err != nil {
		return err
	}
	if replay.ID != stored.ID {
		return ensure(false, "reported probe was not preserved across abort")
	}
	// Revoked leases reject probes and releases.
	_, err = submitProbe(harness, group.LeaseIDs[1], "abort-probe-0002", true, revision.Serial, 15)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[1]+"/release", nil, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// The terminal plan blocks every path that would continue the rollout.
	_, err = allocate(harness, plan.ID, "abort-lease-0002", "east", 1)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": aborted.Version,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/activate", map[string]any{
		"expected_version": aborted.Version,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5002,
		Authority: "ns1.abort.example",
		Entries:   []domain.ZoneEntry{{Owner: "abort.example", Type: domain.EntryA, Value: "192.0.2.20"}},
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// Repeated aborts return the identical fact, even with a different payload.
	same, err := abortPlan(harness, plan.ID, domain.AbortInput{
		Reason:     "authority serving stale serial",
		Initiator:  "ops-oncall",
		RequestKey: "abort-request-0001",
	})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(same, record) {
		return ensure(false, "abort replay returned a different fact")
	}
	other, err := abortPlan(harness, plan.ID, domain.AbortInput{Reason: "a second unrelated reason"})
	if err != nil {
		return err
	}
	if other.ID != record.ID || other.Reason != record.Reason {
		return ensure(false, "second abort created a new fact instead of returning the original")
	}

	// Concurrent aborts race to a single recorded fact.
	const racers = 8
	ids := make([]string, racers)
	var wg sync.WaitGroup
	for index := range ids {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			result, callErr := abortPlan(harness, plan.ID, domain.AbortInput{
				Reason: fmt.Sprintf("concurrent abort %d", slot),
			})
			if callErr == nil {
				ids[slot] = result.ID
			}
		}(index)
	}
	wg.Wait()
	for _, id := range ids {
		if id != record.ID {
			return ensure(false, "concurrent aborts did not converge on one fact")
		}
	}

	// Assessments remain available over the preserved probes.
	var postAbort domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "abort-assess-0002",
	}, &postAbort); err != nil {
		return err
	}
	if postAbort.ProbeCount != 1 {
		return ensure(false, "assessment after abort lost the preserved probe")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != postAbort.ID {
		return ensure(false, "latest assessment after abort does not match")
	}

	// The aborted plan is visible through the aggregate list filter.
	var abortedPage domain.PlanPage
	if err := harness.call(http.MethodGet, "/v1/plans?state=aborted", nil, &abortedPage); err != nil {
		return err
	}
	if len(abortedPage.Items) != 1 || abortedPage.Items[0].ID != plan.ID {
		return ensure(false, "aborted plan is missing from the state filter")
	}

	if err := abortTerminalCases(harness); err != nil {
		return err
	}
	if err := abortAllocationRace(harness); err != nil {
		return err
	}
	return abortProbeRace(harness)
}

// abortTerminalCases pins the mutual exclusion between complete and abort and
// the rejection of aborts on plans that were never active.
func abortTerminalCases(harness *harness) error {
	completed, _, err := createActivePlan(harness, "abort-completed.example", 6001, 1, 1, 100)
	if err != nil {
		return err
	}
	var done domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+completed.ID+"/complete", map[string]any{
		"expected_version": completed.Version,
	}, &done); err != nil {
		return err
	}
	_, err = abortPlan(harness, completed.ID, domain.AbortInput{Reason: "too late to abort"})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	err = harness.call(http.MethodGet, "/v1/plans/"+completed.ID+"/abort", nil, nil)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	var draft domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "abort-draft.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    100,
		ExpectedNodes:       1,
	}, &draft); err != nil {
		return err
	}
	_, err = abortPlan(harness, draft.ID, domain.AbortInput{Reason: "draft cannot abort"})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	_, err = abortPlan(harness, "pln_missing", domain.AbortInput{Reason: "no such plan here"})
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}
	_, err = abortPlan(harness, draft.ID, domain.AbortInput{Reason: "x"})
	return expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed))
}

// abortAllocationRace fires an abort and an allocation at the same active
// plan. Exactly one ordering is possible: either the allocation landed first
// and its leases are revoked by the abort, or the abort landed first and the
// allocation is rejected. Both leave the plan aborted and the nodes ready.
func abortAllocationRace(harness *harness) error {
	if _, err := enrollNode(harness, "resolver-race-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-race-02", "west"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "abort-race.example", 7001, 1, 1, 100)
	if err != nil {
		return err
	}

	start := make(chan struct{})
	var group domain.LeaseGroup
	var allocErr error
	var record domain.PlanAbort
	var abortErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		group, allocErr = allocate(harness, plan.ID, "race-lease-0001", "west", 2)
	}()
	go func() {
		defer wg.Done()
		<-start
		record, abortErr = abortPlan(harness, plan.ID, domain.AbortInput{Reason: "race against allocation"})
	}()
	close(start)
	wg.Wait()
	if abortErr != nil {
		return abortErr
	}

	if allocErr == nil {
		if len(group.LeaseIDs) != 2 {
			return ensure(false, "winning allocation did not return both leases")
		}
		for _, leaseID := range group.LeaseIDs {
			_, err := submitProbe(harness, leaseID, "race-probe-"+leaseID[len(leaseID)-4:], true, 7001, 10)
			if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
				return err
			}
		}
		if len(record.RevokedLeaseIDs) != 2 {
			return ensure(false, "abort did not revoke the leases that won the race")
		}
	} else if err := expectFailure(allocErr, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	} else if len(record.RevokedLeaseIDs) != 0 {
		return ensure(false, "abort recorded leases although the allocation lost the race")
	}

	var readyPage domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=ready&region=west", nil, &readyPage); err != nil {
		return err
	}
	if len(readyPage.Items) != 2 {
		return ensure(false, "race left west nodes leased")
	}
	var current domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &current); err != nil {
		return err
	}
	if current.State != domain.PlanAborted {
		return ensure(false, "race did not end in the aborted state")
	}
	return nil
}

// abortProbeRace fires an abort and a probe submission at the same claimed
// lease. Either the probe lands first and is preserved by the abort, or the
// abort revokes the lease first and the probe is rejected.
func abortProbeRace(harness *harness) error {
	if _, err := enrollNode(harness, "resolver-race-03", "south"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "abort-probe-race.example", 8001, 1, 1, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "race-lease-0002", "south", 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]

	start := make(chan struct{})
	var probeResult domain.Probe
	var probeErr error
	var record domain.PlanAbort
	var abortErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		probeResult, probeErr = submitProbe(harness, leaseID, "race-probe-0001", true, revision.Serial, 10)
	}()
	go func() {
		defer wg.Done()
		<-start
		record, abortErr = abortPlan(harness, plan.ID, domain.AbortInput{Reason: "race against probe"})
	}()
	close(start)
	wg.Wait()
	if abortErr != nil {
		return abortErr
	}

	if probeErr == nil {
		if len(record.RevokedLeaseIDs) != 0 || record.PreservedProbeCount != 1 {
			return ensure(false, "abort fact disagrees with the probe that won the race")
		}
		replay, err := submitProbe(harness, leaseID, "race-probe-0001", true, revision.Serial, 10)
		if err != nil {
			return err
		}
		if replay.ID != probeResult.ID {
			return ensure(false, "probe that won the race was not preserved")
		}
		return nil
	}
	if err := expectFailure(probeErr, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}
	if len(record.RevokedLeaseIDs) != 1 || record.RevokedLeaseIDs[0] != leaseID {
		return ensure(false, "abort fact disagrees with the probe that lost the race")
	}
	return nil
}
