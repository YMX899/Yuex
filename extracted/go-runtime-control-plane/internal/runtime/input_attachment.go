package runtime

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	"huahuoai/backend/source/internal/domain"
)

var runtimeInputResourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// AgentRunInputAttachmentIdentity is the immutable, provider-independent
// identity of one current-turn attachment. Storage references and signed URLs
// are deliberately absent; the Dispatcher resolves them again through the
// owned ResourceIndex before materialization.
type AgentRunInputAttachmentIdentity struct {
	ResourceID  string `json:"resourceId"`
	Usage       string `json:"usage"`
	MIMEType    string `json:"mimeType"`
	SizeBytes   int64  `json:"sizeBytes"`
	SHA256      string `json:"sha256"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	LogicalPath string `json:"logicalPath"`
}

func RuntimeInputAttachmentLogicalPath(index int, mimeType string) (string, error) {
	if index < 0 || index >= 16 {
		return "", domain.ErrorCode("ATTACHMENT_INVALID")
	}
	extension := ""
	switch strings.TrimSpace(strings.ToLower(mimeType)) {
	case "image/jpeg":
		extension = "jpg"
	case "image/png":
		extension = "png"
	case "image/webp":
		extension = "webp"
	default:
		return "", domain.ErrorCode("META_WORKSPACE_INPUT_UNSUPPORTED")
	}
	return fmt.Sprintf("input/attachments/%02d.%s", index+1, extension), nil
}

func ValidateAgentRunInputAttachments(items []AgentRunInputAttachmentIdentity, policy MetaWorkspaceInputPolicy) error {
	if len(policy.AcceptedImageMIMETypes) == 0 {
		if len(items) != 0 {
			return domain.ErrorCode("META_WORKSPACE_INPUT_UNSUPPORTED")
		}
		return nil
	}
	if policy.ImageRequired && len(items) == 0 {
		return domain.ErrorCode("META_WORKSPACE_INPUT_REQUIRED")
	}
	if len(items) > policy.MaxFiles {
		return domain.ErrorCode("META_WORKSPACE_INPUT_UNSUPPORTED")
	}
	allowedMIME := make(map[string]bool, len(policy.AcceptedImageMIMETypes))
	for _, mimeType := range policy.AcceptedImageMIMETypes {
		allowedMIME[mimeType] = true
	}
	seenResources := map[string]bool{}
	seenPaths := map[string]bool{}
	var totalBytes int64
	for index, item := range items {
		expectedPath, err := RuntimeInputAttachmentLogicalPath(index, item.MIMEType)
		if err != nil || item.LogicalPath != expectedPath || path.Clean(item.LogicalPath) != item.LogicalPath ||
			!runtimeInputResourceIDPattern.MatchString(item.ResourceID) || seenResources[item.ResourceID] || seenPaths[item.LogicalPath] ||
			item.Usage != policy.Usage || !allowedMIME[item.MIMEType] || item.SizeBytes < 1 || item.SizeBytes > policy.MaxBytesPerFile ||
			!validSHA256(item.SHA256) || item.Width < 1 || item.Height < 1 || item.Width > policy.MaxWidth || item.Height > policy.MaxHeight ||
			int64(item.Width)*int64(item.Height) > policy.MaxPixels {
			return domain.ErrorCode("ATTACHMENT_INVALID")
		}
		seenResources[item.ResourceID] = true
		seenPaths[item.LogicalPath] = true
		totalBytes += item.SizeBytes
		if totalBytes > policy.MaxBytes {
			return domain.ErrorCode("META_WORKSPACE_INPUT_UNSUPPORTED")
		}
	}
	return nil
}

func SameAgentRunInputAttachmentIdentities(left, right []AgentRunInputAttachmentIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
