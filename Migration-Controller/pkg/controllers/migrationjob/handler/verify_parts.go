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

package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/dragonfly"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ossAccessKeyIDOption / ossAccessKeySecretOption are the OSS credential keys
// in storage options.
//
// Same values as pkg/controllers/preheatjob's OptionAccessKeyID /
// OptionAccessKeySecret; defined separately here to avoid the handler package
// importing the preheatjob package backwards.
const (
	ossAccessKeyIDOption     = "accessKeyId"
	ossAccessKeySecretOption = "accessKeySecret"
)

// verifyCheckpointParts confirms no checkpoint part is missing before
// dispatching restore.
//
// The checkpoint side records the actually-uploaded part count in
// CheckpointAttributes.FileCount, the only authoritative expectation. Both the
// worker and the preheat side discover "which parts actually exist" on storage
// by probing, and pure probing has a blind spot it cannot catch: it cannot
// distinguish "originally 7 parts" from "should be 8 but the tail one is lost"
// - with 00..06 present and 07 missing the indices are still continuous and the
// continuity check passes, so only 7 parts get downloaded and the container
// silently misses files. Comparing against the expectation exposes it.
//
// Currently only storageType=dragonfly goes through this comparison
// (checkpointDirURL can derive the OSS directory URL and credentials; the
// storage.OSS case at the entry is a reserved implementation, currently
// unreachable). local / nfs / pvc are paths the controller cannot see; they
// are left to the worker-side DiscoverLocalParts continuity check, which keeps
// the tail blind spot.
func (h *PrepareDataHandler) verifyCheckpointParts(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, cms *v1beta1.ContainerMigrationStatus) error {
	if jobReq.Spec.Storage.Dragonfly == nil {
		return nil
	}

	ctxLog := log.FromContext(ctx)

	expected := int(cms.CheckpointAttributes.FileCount)
	if expected <= 0 {
		// The checkpoint side did not record the part count (should not happen); fall back to "pass as long as probing finds it".
		ctxLog.Info("checkpoint attributes carry no part count, skipping the expected-count check",
			"container", cms.Name)
		return nil
	}

	dirURL, accessKeyID, accessKeySecret, ok := checkpointDirURL(jobReq.Spec.Storage, cms.CheckpointAttributes.Path)
	if !ok {
		ctxLog.Info("storage backend is not probeable from the controller, "+
			"leaving part discovery entirely to the worker", "container", cms.Name)
		return nil
	}

	indices, err := dragonfly.DiscoverParts(ctx, dirURL, accessKeyID, accessKeySecret)
	if err != nil {
		return err
	}
	if len(indices) != expected {
		return fmt.Errorf("expected %d part(s) under %s but found %d: "+
			"the checkpoint upload is incomplete or some part was removed",
			expected, cms.CheckpointAttributes.Path, len(indices))
	}

	ctxLog.V(4).Info("checkpoint parts verified", "container", cms.Name, "parts", len(indices))
	return nil
}

// checkpointDirURL joins the storage config with the checkpoint-relative path
// into the OSS directory URL and extracts the credentials.
//
// The joined path must be exactly identical to the one used by the worker
// download and the controller preheat, otherwise the probe result is
// meaningless (nothing found would be misjudged as missing parts). All three
// are <mountPoint>/<checkpointPath>/part-NN.tar.
func checkpointDirURL(storage *v1beta1.JobStorage, checkpointPath string) (dirURL, ak, sk string, ok bool) {
	if storage == nil {
		return "", "", "", false
	}

	var mountPoint, endpoint string
	var options map[string]string
	switch {
	case storage.Dragonfly != nil:
		mountPoint, endpoint, options = storage.Dragonfly.OSSMountPoint, storage.Dragonfly.OSSEndpoint, storage.Dragonfly.Options
	case storage.OSS != nil:
		mountPoint, endpoint, options = storage.OSS.MountPoint, storage.OSS.Endpoint, storage.OSS.Options
	default:
		return "", "", "", false
	}
	if mountPoint == "" || endpoint == "" || options == nil {
		return "", "", "", false
	}

	ak, sk = options[ossAccessKeyIDOption], options[ossAccessKeySecretOption]
	if ak == "" || sk == "" {
		return "", "", "", false
	}

	// mountPoint looks like oss://<bucket>/<basePath> or <bucket>/<basePath>.
	bucket, basePath, ok := splitOSSMountPoint(mountPoint)
	if !ok {
		return "", "", "", false
	}

	objectPath := strings.Trim(strings.Join([]string{basePath, checkpointPath}, "/"), "/")
	return fmt.Sprintf("https://%s.%s/%s", bucket, endpoint, objectPath), ak, sk, true
}

// splitOSSMountPoint splits the bucket and basePath out of the mount point.
func splitOSSMountPoint(mountPoint string) (bucket, basePath string, ok bool) {
	s := strings.TrimPrefix(mountPoint, "oss://")
	s = strings.Trim(s, "/")
	if s == "" {
		return "", "", false
	}
	bucket, basePath, found := strings.Cut(s, "/")
	if bucket == "" {
		return "", "", false
	}
	if !found {
		basePath = ""
	}
	return bucket, basePath, true
}
