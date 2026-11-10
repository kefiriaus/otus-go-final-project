// Package collector runs independent typed measurement sources and short commands.
// Sources must honor context cancellation and transfer immutable results to callers.
// Callers own result channels and must wait for senders before closing them.
// Sampling errors are recoverable; worker termination stops the containing group.
package collector
