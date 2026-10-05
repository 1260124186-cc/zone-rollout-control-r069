package plan

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Create(ctx context.Context, input domain.PlanCreateInput) (domain.RolloutPlan, error) {
	if err := domain.ValidatePlanInput(input); err != nil {
		return domain.RolloutPlan{}, err
	}
	input = domain.NormalizePlanInput(input)
	now := s.clock.Now().UTC()
	plan := domain.RolloutPlan{
		ID:                  s.ids.New("pln"),
		Zone:                input.Zone,
		State:               domain.PlanDraft,
		MinimumSuccessRatio: input.MinimumSuccessRatio,
		MaximumLatencyMS:    input.MaximumLatencyMS,
		ExpectedNodes:       input.ExpectedNodes,
		Version:             1,
		CreatedAt:           now,
	}
	err := s.store.Write(ctx, func(state *domain.State) error {
		state.Plans[plan.ID] = plan
		store.AddAudit(state, s.ids, "plan", plan.ID, "created", now, map[string]any{
			"zone": plan.Zone,
		})
		return nil
	})
	return plan, err
}

func (s *Service) AddRevision(
	ctx context.Context,
	planID string,
	input domain.RevisionCreateInput,
) (domain.ZoneRevision, error) {
	normalized, err := domain.NormalizeRevisionInput(input)
	if err != nil {
		return domain.ZoneRevision{}, err
	}
	if err := domain.ValidateRevisionInput(normalized); err != nil {
		return domain.ZoneRevision{}, err
	}
	now := s.clock.Now().UTC()
	var created domain.ZoneRevision
	err = s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanDraft {
			return domain.InvalidState("revisions can be added only while plan is draft", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		for _, revision := range state.Revisions {
			if revision.PlanID != planID {
				continue
			}
			if revision.Serial == normalized.Serial {
				return domain.NewError(domain.CodeDuplicateRevision, "plan already contains this serial", map[string]any{
					"plan_id": planID,
					"serial":  normalized.Serial,
				})
			}
			if revision.Serial > normalized.Serial {
				return domain.Validation("revision serial must increase", map[string]any{
					"serial": normalized.Serial,
					"latest": revision.Serial,
				})
			}
		}
		created = domain.ZoneRevision{
			ID:         s.ids.New("rev"),
			PlanID:     planID,
			Serial:     normalized.Serial,
			Authority:  normalized.Authority,
			Entries:    append([]domain.ZoneEntry(nil), normalized.Entries...),
			EntryCount: len(normalized.Entries),
			CreatedAt:  now,
		}
		state.Revisions[created.ID] = created
		plan.LatestRevisionID = created.ID
		plan.Version++
		state.Plans[planID] = plan
		store.AddAudit(state, s.ids, "revision", created.ID, "added", now, map[string]any{
			"plan_id": planID,
			"serial":  created.Serial,
		})
		return nil
	})
	return created, err
}

func (s *Service) Activate(
	ctx context.Context,
	planID string,
	expectedVersion uint64,
) (domain.RolloutPlan, error) {
	var plan domain.RolloutPlan
	err := s.store.Write(ctx, func(state *domain.State) error {
		current, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if current.Version != expectedVersion {
			return versionConflict(planID, expectedVersion, current.Version)
		}
		now := s.clock.Now().UTC()
		if err := domain.ActivatePlan(&current, now); err != nil {
			return err
		}
		state.Plans[planID] = current
		store.AddAudit(state, s.ids, "plan", planID, "activated", now, map[string]any{
			"version": current.Version,
		})
		plan = current
		return nil
	})
	return plan, err
}

func (s *Service) Complete(
	ctx context.Context,
	planID string,
	expectedVersion uint64,
) (domain.RolloutPlan, error) {
	var plan domain.RolloutPlan
	err := s.store.Write(ctx, func(state *domain.State) error {
		current, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if current.Version != expectedVersion {
			return versionConflict(planID, expectedVersion, current.Version)
		}
		now := s.clock.Now().UTC()
		if err := domain.CompletePlan(&current, now); err != nil {
			return err
		}
		state.Plans[planID] = current
		store.AddAudit(state, s.ids, "plan", planID, "completed", now, map[string]any{
			"version": current.Version,
		})
		plan = current
		return nil
	})
	return plan, err
}

// Abort moves an active plan into the terminal aborted state inside a single
// store transaction: every still-claimed lease of the plan is revoked and its
// node returned to ready, while reported leases, stored probes, and
// assessments are preserved. The abort fact is recorded once per plan, so a
// repeated abort returns the identical record instead of a new transition.
func (s *Service) Abort(
	ctx context.Context,
	planID string,
	input domain.AbortInput,
) (domain.PlanAbort, error) {
	if err := domain.ValidateAbortInput(input); err != nil {
		return domain.PlanAbort{}, err
	}
	input = domain.NormalizeAbortInput(input)
	now := s.clock.Now().UTC()
	var record domain.PlanAbort
	err := s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State == domain.PlanAborted {
			existing, found := state.AbortRecords[plan.AbortID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "plan references a missing abort record", map[string]any{
					"abort_id": plan.AbortID,
				})
			}
			record = existing
			return nil
		}
		if input.ExpectedVersion > 0 && plan.Version != input.ExpectedVersion {
			return versionConflict(planID, input.ExpectedVersion, plan.Version)
		}
		if err := domain.AbortPlan(&plan, now); err != nil {
			return err
		}

		revoked := make([]string, 0)
		for leaseID, lease := range state.Leases {
			if lease.PlanID == planID && lease.State == domain.LeaseClaimed {
				revoked = append(revoked, leaseID)
			}
		}
		sort.Strings(revoked)
		for _, leaseID := range revoked {
			lease := state.Leases[leaseID]
			finishedAt := now
			lease.State = domain.LeaseRevoked
			lease.FinishedAt = &finishedAt
			state.Leases[leaseID] = lease
			if node, ok := state.Nodes[lease.NodeID]; ok && node.State == domain.NodeLeased {
				node.State = domain.NodeReady
				node.Version++
				node.UpdatedAt = now
				state.Nodes[node.ID] = node
			}
		}
		preservedProbes := 0
		for _, probe := range state.Probes {
			if probe.PlanID == planID {
				preservedProbes++
			}
		}

		record = domain.PlanAbort{
			ID:                  s.ids.New("abt"),
			PlanID:              planID,
			Reason:              input.Reason,
			Initiator:           input.Initiator,
			RequestKey:          input.RequestKey,
			RevokedLeaseIDs:     revoked,
			PreservedProbeCount: preservedProbes,
			AbortedAt:           now,
		}
		state.AbortRecords[record.ID] = record
		plan.AbortID = record.ID
		state.Plans[planID] = plan
		for _, leaseID := range revoked {
			store.AddAudit(state, s.ids, "lease", leaseID, "revoked", now, map[string]any{
				"plan_id":  planID,
				"abort_id": record.ID,
			})
		}
		store.AddAudit(state, s.ids, "plan", planID, "aborted", now, map[string]any{
			"abort_id":              record.ID,
			"reason":                record.Reason,
			"initiator":             record.Initiator,
			"revoked_lease_count":   len(revoked),
			"preserved_probe_count": preservedProbes,
			"version":               plan.Version,
		})
		return nil
	})
	return record, err
}

func (s *Service) GetAbort(ctx context.Context, planID string) (domain.PlanAbort, error) {
	var record domain.PlanAbort
	err := s.store.Read(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanAborted || plan.AbortID == "" {
			return domain.NotFound("abort", planID)
		}
		found, ok := state.AbortRecords[plan.AbortID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing abort record", map[string]any{
				"abort_id": plan.AbortID,
			})
		}
		record = found
		return nil
	})
	return record, err
}

func (s *Service) Get(ctx context.Context, planID string) (domain.RolloutPlan, error) {
	var plan domain.RolloutPlan
	err := s.store.Read(ctx, func(state *domain.State) error {
		found, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		plan = found
		return nil
	})
	return plan, err
}

func (s *Service) LatestRevision(
	ctx context.Context,
	planID string,
) (domain.ZoneRevision, error) {
	var revision domain.ZoneRevision
	err := s.store.Read(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.LatestRevisionID == "" {
			return domain.NotFound("revision", planID)
		}
		found, ok := state.Revisions[plan.LatestRevisionID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing revision", map[string]any{
				"revision_id": plan.LatestRevisionID,
			})
		}
		revision = found
		return nil
	})
	return revision, err
}

func (s *Service) List(
	ctx context.Context,
	stateFilter domain.PlanState,
	limit int,
	cursor string,
) (domain.PlanPage, error) {
	if limit < 1 || limit > 100 {
		return domain.PlanPage{}, domain.Validation("limit must be between 1 and 100", map[string]any{
			"limit": limit,
		})
	}
	after, err := platform.DecodeCursor(cursor)
	if err != nil {
		return domain.PlanPage{}, domain.Validation("cursor is invalid", nil)
	}
	var page domain.PlanPage
	err = s.store.Read(ctx, func(state *domain.State) error {
		items := make([]domain.RolloutPlan, 0, len(state.Plans))
		for _, plan := range state.Plans {
			if stateFilter != "" && plan.State != stateFilter {
				continue
			}
			if after != "" && plan.ID <= after {
				continue
			}
			items = append(items, plan)
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].ID < items[j].ID
		})
		page.Meta.Limit = limit
		if len(items) > limit {
			page.Items = append(page.Items, items[:limit]...)
			page.Meta.NextCursor = platform.EncodeCursor(items[limit-1].ID)
			return nil
		}
		page.Items = append(page.Items, items...)
		return nil
	})
	return page, err
}

func (s *Service) RevisionExists(
	ctx context.Context,
	planID string,
	serial uint32,
	authority string,
	entries []domain.ZoneEntry,
) (bool, error) {
	exists := false
	err := s.store.Read(ctx, func(state *domain.State) error {
		for _, revision := range state.Revisions {
			if revision.PlanID != planID || revision.Serial != serial || revision.Authority != authority {
				continue
			}
			if reflect.DeepEqual(revision.Entries, entries) {
				exists = true
				return nil
			}
		}
		return nil
	})
	return exists, err
}

func RevisionDigest(revision domain.ZoneRevision) string {
	payload, _ := json.Marshal(struct {
		Serial    uint32
		Authority string
		Entries   []domain.ZoneEntry
	}{
		Serial:    revision.Serial,
		Authority: revision.Authority,
		Entries:   revision.Entries,
	})
	return string(payload)
}

func versionConflict(id string, expected uint64, actual uint64) *domain.DomainError {
	return domain.NewError(domain.CodeVersionConflict, "plan version does not match", map[string]any{
		"plan_id":  id,
		"expected": expected,
		"actual":   actual,
	})
}
