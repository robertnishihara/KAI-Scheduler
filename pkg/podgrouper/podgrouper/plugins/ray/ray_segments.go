// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package ray

import (
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"

	"github.com/kai-scheduler/KAI-scheduler/pkg/podgrouper/podgroup"
	"github.com/kai-scheduler/KAI-scheduler/pkg/podgrouper/podgrouper/plugins/constants"
)

const maxSegmentsPerWorkerGroup = 10000

// segmentation splits a worker group's pods into fixed-size segments, each scheduled as its own
// SubGroup with the segment topology constraints. Pods are assigned to segments by their position
// in the group, derived from the replica (and host) index labels that KubeRay sets on worker pods.
type segmentation struct {
	size                int
	numPods             int
	numSegments         int
	numOfHosts          int
	topologyConstraints *podgroup.TopologyConstraintMetadata
}

func segmentSubGroupName(groupName string, segmentIndex int) string {
	return fmt.Sprintf("%s-segment-%d", groupName, segmentIndex)
}

// getWorkerGroupSegmentation returns nil if the worker group's pod template doesn't request segmentation.
func getWorkerGroupSegmentation(
	groupSpec interface{}, groupName string, desiredReplicas, numOfHosts int64,
	rayClusterObj, topOwner *unstructured.Unstructured,
) (*segmentation, error) {
	workerGroupSpec, ok := groupSpec.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to parse worker group spec %s", groupName)
	}
	annotations, _, err := unstructured.NestedStringMap(workerGroupSpec, "template", "metadata", "annotations")
	if err != nil {
		return nil, err
	}

	sizeStr := annotations[constants.SegmentSizeKey]
	if sizeStr == "" {
		return nil, nil
	}
	size, err := strconv.Atoi(sizeStr)
	if err != nil || size <= 0 {
		return nil, fmt.Errorf("invalid %s %q on workerGroupSpecs.%s: must be a positive integer",
			constants.SegmentSizeKey, sizeStr, groupName)
	}

	numPods := int(desiredReplicas * numOfHosts)
	numSegments := (numPods + size - 1) / size
	if numSegments == 0 {
		return nil, nil
	}
	if numSegments > maxSegmentsPerWorkerGroup {
		return nil, fmt.Errorf("workerGroupSpecs.%s would be split into %d segments, more than the maximum of %d",
			groupName, numSegments, maxSegmentsPerWorkerGroup)
	}

	topologyConstraints, err := getSegmentTopologyConstraints(annotations, groupName, rayClusterObj, topOwner)
	if err != nil {
		return nil, err
	}

	return &segmentation{
		size:                size,
		numPods:             numPods,
		numSegments:         numSegments,
		numOfHosts:          int(numOfHosts),
		topologyConstraints: topologyConstraints,
	}, nil
}

// getSegmentTopologyConstraints reads the segment placement from the worker group's pod template. The
// topology name may come from the pod template, the RayCluster, or the top owner (e.g. the RayJob).
func getSegmentTopologyConstraints(
	templateAnnotations map[string]string, groupName string, owners ...*unstructured.Unstructured,
) (*podgroup.TopologyConstraintMetadata, error) {
	required := templateAnnotations[constants.SegmentTopologyRequiredPlacementKey]
	preferred := templateAnnotations[constants.SegmentTopologyPreferredPlacementKey]
	if required == "" && preferred == "" {
		return nil, nil
	}

	topology := templateAnnotations[constants.TopologyKey]
	for _, owner := range owners {
		if topology != "" {
			break
		}
		topology = owner.GetAnnotations()[constants.TopologyKey]
	}
	if topology == "" {
		return nil, fmt.Errorf("topology annotation (%s) is required for workerGroupSpecs.%s when segment placement annotations are set",
			constants.TopologyKey, groupName)
	}

	return &podgroup.TopologyConstraintMetadata{
		Topology:               topology,
		RequiredTopologyLevel:  required,
		PreferredTopologyLevel: preferred,
	}, nil
}

// buildSubGroups returns the worker group's SubGroup, which becomes a parent, followed by one leaf
// SubGroup per segment. The group's minimum is filled segment by segment, so the leading segments
// are mandatory and the trailing ones are elastic (MinAvailable 0).
func (s *segmentation) buildSubGroups(
	groupName string, groupMinAvailable int32, groupTopologyConstraints *podgroup.TopologyConstraintMetadata,
) []*podgroup.SubGroupMetadata {
	// Parent SubGroups must use MinSubGroup (webhook rejects minMember on non-leaf).
	subGroups := []*podgroup.SubGroupMetadata{{
		Name:                groupName,
		MinSubGroup:         ptr.To(int32(s.numSegments)),
		TopologyConstraints: groupTopologyConstraints,
	}}
	for i := range s.numSegments {
		podsInSegment := min(s.size, s.numPods-i*s.size)
		minAvailable := max(0, min(int(groupMinAvailable)-i*s.size, podsInSegment))
		subGroups = append(subGroups, &podgroup.SubGroupMetadata{
			Name:                segmentSubGroupName(groupName, i),
			Parent:              ptr.To(groupName),
			MinAvailable:        int32(minAvailable),
			TopologyConstraints: s.topologyConstraints,
		})
	}
	return subGroups
}

func (s *segmentation) podSegmentIndex(pod *v1.Pod) (int, error) {
	replicaIndex, err := getIndexLabel(pod, utils.RayWorkerReplicaIndexKey)
	if err != nil {
		return 0, fmt.Errorf("%w (segmented Ray worker groups require KubeRay's RayMultiHostIndexing feature gate)", err)
	}
	hostIndex := 0
	if s.numOfHosts > 1 {
		if hostIndex, err = getIndexLabel(pod, utils.RayHostIndexKey); err != nil {
			return 0, err
		}
	}

	segmentIndex := (replicaIndex*s.numOfHosts + hostIndex) / s.size
	// KubeRay creates workers with the lowest free replica index, so an index past the current
	// replica count can only belong to a pod that outlived a scale-down. Keep it in the last
	// segment rather than referencing a segment that doesn't exist.
	return min(segmentIndex, s.numSegments-1), nil
}

func getIndexLabel(pod *v1.Pod, key string) (int, error) {
	value, found := pod.Labels[key]
	if !found {
		return 0, fmt.Errorf("index label %s not found on pod %s/%s", key, pod.Namespace, pod.Name)
	}
	index, err := strconv.Atoi(value)
	if err != nil || index < 0 {
		return 0, fmt.Errorf("invalid index label %s=%q on pod %s/%s", key, value, pod.Namespace, pod.Name)
	}
	return index, nil
}
