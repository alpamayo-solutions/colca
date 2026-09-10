package grantsync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// definitionTTL bounds how long a definition command stays executable. It is a
// command envelope requirement, not a property of the definition it carries:
// once applied, the definition lives until it is superseded or retracted.
const definitionTTL = 5 * time.Minute

// Report is what one cycle did and what it saw but did not fix.
type Report struct {
	Changes  []string
	Problems []string
}

func (r *Report) change(format string, args ...any) {
	r.Changes = append(r.Changes, fmt.Sprintf(format, args...))
}

func (r *Report) problem(format string, args ...any) {
	r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
}

// Syncer runs cycles. It holds no state between them beyond its clients: both
// stores are durable and every cycle reads them whole, so two instances racing
// produce the same writes and a crash mid-cycle is a cycle that gets redone.
type Syncer struct {
	Node     *NodeClient
	KC       *Keycloak
	Owner    string // deployment key stamped into the managed marker
	RootULID string
	DryRun   bool
	Log      *slog.Logger
	Metrics  *Metrics
}

func (s *Syncer) logger() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// Once runs one full read, diff and write. Both reads complete before any
// write: an unreachable or incomplete source is not an empty one, and treating
// it as empty would revoke access or retire resources. A partial view skips the
// cycle; the next one reads everything again.
func (s *Syncer) Once(ctx context.Context) (Report, error) {
	clientUUID, err := s.KC.ClientUUID(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("keycloak read failed, nothing written: %w", err)
	}
	view, err := s.KC.View(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("keycloak read failed, nothing written: %w", err)
	}
	tree, err := s.Node.Tree(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("tree read failed, nothing written: %w", err)
	}

	var report Report
	for _, p := range view.Problems {
		report.problem("%s", p)
	}
	grants, problems := CompileGrants(view.Permissions, view.Attributes)
	for _, p := range problems {
		report.problem("%s", p)
	}

	if err := s.applyResources(ctx, clientUUID, tree, view.Resources, &report); err != nil {
		return report, err
	}
	if err := s.applyDefinitions(ctx, grants, tree, &report); err != nil {
		return report, err
	}
	s.Metrics.observeProblems(len(report.Problems))
	return report, nil
}

func (s *Syncer) applyResources(
	ctx context.Context, clientUUID string, tree TreeView, existing []Resource, report *Report,
) error {
	plan := PlanResources(tree.Elements, existing, s.Owner)
	for _, name := range plan.Unmanaged {
		report.problem("authz resource %s carries no %s marker — left alone", name, ManagedByAttr)
	}
	if len(plan.Create)+len(plan.Update) > 0 && !s.DryRun {
		if err := s.KC.EnsureScopes(ctx, clientUUID); err != nil {
			return err
		}
	}
	for _, r := range plan.Create {
		report.change("keycloak: register resource %s (%s)", r.Name, r.DisplayName)
		if s.DryRun {
			continue
		}
		if err := s.KC.CreateResource(ctx, clientUUID, r); err != nil {
			return err
		}
		s.Metrics.resourceRegistered()
	}
	for _, r := range plan.Update {
		report.change("keycloak: relabel resource %s → %s", r.Name, r.DisplayName)
		if s.DryRun {
			continue
		}
		if err := s.KC.UpdateResource(ctx, clientUUID, r); err != nil {
			return err
		}
		s.Metrics.resourceRegistered()
	}
	for _, r := range plan.Delete {
		report.change("keycloak: remove resource %s (element retired)", r.Name)
		if s.DryRun {
			continue
		}
		if err := s.KC.DeleteResource(ctx, clientUUID, r.ID); err != nil {
			return err
		}
		s.Metrics.resourceRemoved()
	}
	return nil
}

func (s *Syncer) applyDefinitions(
	ctx context.Context, desired map[string][]string, tree TreeView, report *Report,
) error {
	plan := PlanDefinitions(desired, tree, s.RootULID)
	for _, def := range plan.Upsert {
		report.change("colca: define group %s with %d grant(s)", def.ID, len(def.Grants))
		if s.DryRun {
			continue
		}
		topic := uns.Prefix() + "_CmdConfigure/" + s.RootULID + "/definition/upsert"
		body := envelope(map[string]any{
			"definitions": []map[string]any{{"contract": "_Group", "definition": def}},
		})
		if err := s.Node.Publish(ctx, topic, body); err != nil {
			return err
		}
		s.Metrics.definitionWritten()
	}
	for _, id := range plan.Retract {
		report.change("colca: retract group %s (no longer granted in Keycloak)", id)
		if s.DryRun {
			continue
		}
		topic := uns.Prefix() + "_CmdConfigure/" + s.RootULID + "/definition/delete"
		body := envelope(map[string]any{
			"definitions": []map[string]any{{"contract": "_Group", "id": id}},
		})
		if err := s.Node.Publish(ctx, topic, body); err != nil {
			return err
		}
		s.Metrics.definitionRetracted()
	}
	for _, id := range plan.Foreign {
		report.problem("group %s is defined by another node and Keycloak does not account for it", id)
	}
	for _, orphan := range plan.Unresolvable {
		report.problem("group %s grants on element %s, which no node in this tree holds",
			orphan.Group, orphan.Element)
	}
	return nil
}

func envelope(body map[string]any) map[string]any {
	out := map[string]any{
		"correlation_id": "grant-sync-" + randomID(),
		"expires_at":     time.Now().Add(definitionTTL).UnixMilli(),
	}
	for k, v := range body {
		out[k] = v
	}
	return out
}

func randomID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0"
	}
	return hex.EncodeToString(b[:])
}

// Run polls until ctx is cancelled. A failed cycle is logged and retried, never
// fatal.
func (s *Syncer) Run(ctx context.Context, every time.Duration) {
	log := s.logger()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		report, err := s.Once(ctx)
		switch {
		case err != nil && ctx.Err() != nil:
			return // shutting down: not a failure worth logging as one
		case err != nil:
			s.Metrics.cycle("failed")
			log.Error("sync cycle failed, nothing written", "error", err)
		default:
			s.Metrics.cycle("ok")
			for _, change := range report.Changes {
				log.Info(change)
			}
			for _, problem := range report.Problems {
				log.Warn(problem)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Summary renders a report for a one-shot run.
func (r Report) Summary() string {
	if len(r.Changes) == 0 && len(r.Problems) == 0 {
		return "already in sync — no changes"
	}
	var b strings.Builder
	for _, c := range r.Changes {
		b.WriteString("  " + c + "\n")
	}
	for _, p := range r.Problems {
		b.WriteString("  WARN: " + p + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}
