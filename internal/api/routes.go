// Package api implements the node-local administrative HTTP/JSON interface.

package api

import (
	"net/http"

	"github.com/gorilla/mux"
)

// Routes wires the admin API routes.
func Routes(s *Server) *mux.Router {
	r := mux.NewRouter()

	// Health check
	r.HandleFunc("/healthz", s.handleHealth).Methods("GET")

	// API routes
	r.HandleFunc("/api/v1/status", s.handleStatus).Methods("GET")
	r.HandleFunc("/api/v1/peers", s.handlePeers).Methods("GET")
	r.HandleFunc("/api/v1/capabilities", s.handleCapabilities).Methods("GET")
	r.HandleFunc("/api/v1/skills", s.handleSkills).Methods("GET")
	r.HandleFunc("/api/v1/skills", s.handleUpsertSkill).Methods("POST")
	r.HandleFunc("/api/v1/skills/{name}", s.handleDropSkillDoc).Methods("DELETE")
	r.HandleFunc("/api/v1/skills/sync", s.handleSyncSkills).Methods("POST")
	r.HandleFunc("/api/v1/tasks", s.handleSubmit).Methods("POST")
	r.HandleFunc("/api/v1/tasks", s.handleList).Methods("GET")
	r.HandleFunc("/api/v1/tasks/{id}", s.handleGetTask).Methods("GET")
	r.HandleFunc("/api/v1/tasks/{id}/cancel", s.handleCancel).Methods("POST")
	r.HandleFunc("/api/v1/tasks/{id}", s.handleCancel).Methods("DELETE")
	r.HandleFunc("/api/v1/tasks/{id}/resubmit", s.handleResubmit).Methods("POST")
	r.HandleFunc("/api/v1/config", s.handleConfigView).Methods("GET")
	r.HandleFunc("/api/v1/triggers", s.handleListTriggers).Methods("GET")
	r.HandleFunc("/api/v1/triggers", s.handleUpsertTrigger).Methods("POST")
	r.HandleFunc("/api/v1/triggers/{id}", s.handleDeleteTrigger).Methods("DELETE")
	r.HandleFunc("/api/v1/rebinds", s.handleRebinds).Methods("GET")
	r.HandleFunc("/api/v1/admin/reload-config", s.handleReloadConfig).Methods("POST")
	r.HandleFunc("/api/v1/admin/rotate-key", s.handleRotateKey).Methods("POST")
	r.HandleFunc("/api/v1/admin/revoke", s.handleRevoke).Methods("POST")
	r.HandleFunc("/api/v1/admin/leave", s.handleLeave).Methods("POST")
	r.HandleFunc("/api/v1/admin/scan", s.handleScan).Methods("POST")
	r.HandleFunc("/api/v1/admin/scan", s.handleScanLast).Methods("GET")
	r.HandleFunc("/api/v1/candidates/promote", s.PromoteCandidate).Methods("POST")

	return r
}