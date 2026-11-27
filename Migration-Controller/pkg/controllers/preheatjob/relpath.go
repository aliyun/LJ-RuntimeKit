/*
Copyright (c) 2024-2026 Alibaba Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package preheatjob

import (
	"fmt"
	"strings"
)

// validRelPath validates the structure of "path relative to the storage root"
// fields and returns the canonical value with leading/trailing slashes
// stripped.
//
// This governs PreheatJob's spec.pvc.pvcPath and the path prefix on the PV
// (csi.volumeAttributes["path"] / nfs.path). Both were previously strings.Trim(p,
// "/") then joined directly into three consumers: the preheat source pod's
// shell script, the http remoteURL, and the EFC warmup target_path - the shell
// one was unquoted interpolation, so a value containing ";" could execute
// arbitrary commands inside the privileged preheat source pod.
//
// Deliberately no character whitelist: checkpoint data may be moved by users
// to self-made directories, so any character inside a path segment (spaces,
// brackets, CJK, "$", ";", etc.) is allowed. The safety of these characters is
// held by each consumer itself - the shell passes values via positional
// parameters, remoteURL is built with url.URL escaping, target_path goes
// through query.Encode, and the worker side has sanitizeEFCRelPath when
// reading status.efcPath.
//
// Therefore only three structural problems are rejected:
//   - "." / ".." segments: after joining, filepath.Join and URL resolution
//     lexically resolve them, letting the path escape the storage root (a PV is
//     a cluster-scoped object whose path is written by static yaml or the
//     provisioner; K8s performs no validation on volumeAttributes values, so
//     ".." can legally exist);
//   - empty segments ("//"): segment boundary ambiguity, and Clean would eat
//     them making the validated value differ from the actual one;
//   - backslashes: mixing in Windows-style separators creates segment boundary
//     ambiguity downstream.
//
// An empty value is structurally legal and returns ("", nil); whether empty
// is allowed is up to the caller: an empty pvcPath is a user input error,
// while an empty PV.path means the PVC is mounted at the bucket root and the
// prefix should indeed be empty.
//
// The criteria align with the worker side's sanitizeEFCRelPath in
// pkg/storage (both validate the same set of fields; inconsistent rules would
// let one side pass a value the other degrades): hidden directory names
// (.hidden) and any character inside a segment are legal, only "."/".."
// traversal and backslashes are rejected. The only difference between the two
// sides is the failure semantics: here it is an admission gate (illegal ->
// markFailed / error), on the worker it is a safety net (illegal -> degrade,
// not blocking the restore main path).
func validRelPath(raw string) (string, error) {
	s := strings.Trim(raw, "/")
	if s == "" {
		return "", nil
	}
	if strings.ContainsRune(s, '\\') {
		return "", fmt.Errorf("must not contain backslashes")
	}
	for _, seg := range strings.Split(s, "/") {
		switch seg {
		case "":
			return "", fmt.Errorf(`must not contain empty segments ("//")`)
		case ".", "..":
			return "", fmt.Errorf("must not contain %q segments", seg)
		}
	}
	return s, nil
}
