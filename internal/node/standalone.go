package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
	"strings"
	"time"
)

// Called before listeners or pruning. A restart resumes the same transition;
// removing the flag cannot resume an old parent or restore retired credentials.
func prepareStandalone(cfg *config.Config, st *store.Store, reg *registry.Manager) error {
	state, err := st.StandaloneGet()
	if err != nil {
		return err
	}
	if state != nil && (!cfg.Standalone || cfg.Parent != nil) {
		return fmt.Errorf("standalone trust was retired: automatic fleet rejoin is refused")
	}
	if !cfg.Standalone {
		return nil
	}
	if cfg.Parent != nil {
		return fmt.Errorf("standalone cannot have a parent")
	}
	if state == nil {
		state = &store.StandaloneState{Since: time.Now().Unix() + 1, PATs: map[string]bool{}, Pending: true}
		if raw, known := st.AncestryGet(); known {
			var ancestry uns.Ancestry
			if err := json.Unmarshal(raw, &ancestry); err != nil {
				return fmt.Errorf("cannot preserve operator scope from corrupt ancestry: %w", err)
			}
			for _, ancestor := range ancestry {
				if ancestor.Element != "" {
					state.FormerAncestors = append(state.FormerAncestors, ancestor.Element)
				}
			}
		}
		for after := ""; ; {
			rows, next, err := st.KVScanPage("", after, 1000, []string{uns.PersonalAccessTokenContract})
			if err != nil {
				return err
			}
			for _, row := range rows {
				state.PATs[row.Path] = true
			}
			if next == "" {
				break
			}
			after = next
		}
		for _, entry := range reg.List() {
			if !entry.IsLocal() {
				state.Identities = append(state.Identities, entry.ULID)
			}
		}
		if err := st.StandalonePut(state); err != nil {
			return err
		}
	}
	if state.Pending {
		for _, id := range state.Identities {
			if _, _, err := reg.Revoke(id); err != nil && !errors.Is(err, registry.ErrNotEnrolled) {
				return err
			}
		}
		for _, cur := range st.Cursors() {
			if strings.HasPrefix(cur.Name, "up:") || strings.HasPrefix(cur.Name, "down:") || strings.HasPrefix(cur.Name, "down-def:") || cur.Name == "uplink" || cur.Name == "downlink" || cur.Name == "downlink-def" {
				if err := st.CursorDelete(cur.Name, cur.Stream); err != nil {
					return err
				}
			}
		}
		state.Pending = false
		if err := st.StandalonePut(state); err != nil {
			return err
		}
	}
	cfg.StandaloneSince, cfg.RetiredPATs = state.Since, state.PATs
	cfg.API.Token = "" // the former fleet's static administrative credential
	return nil
}
