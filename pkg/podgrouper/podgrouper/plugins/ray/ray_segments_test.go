// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package ray

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"

	"github.com/kai-scheduler/KAI-scheduler/pkg/podgrouper/podgroup"
	"github.com/kai-scheduler/KAI-scheduler/pkg/podgrouper/podgrouper/plugins/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/podgrouper/podgrouper/plugins/defaultgrouper"
)

func segmentedRayCluster(clusterAnnotations map[string]interface{}, workerGroup map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "RayCluster",
			"apiVersion": "ray.io/v1",
			"metadata": map[string]interface{}{
				"name":        "segmented",
				"namespace":   "test_namespace",
				"uid":         "1",
				"annotations": clusterAnnotations,
			},
			"spec": map[string]interface{}{
				"headGroupSpec":    map[string]interface{}{},
				"workerGroupSpecs": []interface{}{workerGroup},
			},
		},
	}
}

func segmentedWorkerGroup(replicas, minReplicas int64, templateAnnotations map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"groupName":   "workers",
		"replicas":    replicas,
		"minReplicas": minReplicas,
		"template": map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": templateAnnotations,
			},
		},
	}
}

func rayWorkerPod(name string, labels map[string]string) *v1.Pod {
	podLabels := map[string]string{utils.RayNodeGroupLabelKey: "workers"}
	for k, v := range labels {
		podLabels[k] = v
	}
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test_namespace", Labels: podLabels}}
}

func getSegmentedMetadata(t *testing.T, rayCluster *unstructured.Unstructured, pod *v1.Pod) (*podgroup.Metadata, error) {
	t.Helper()
	client := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithRuntimeObjects(rayCluster).Build()
	grouper := NewRayClusterGrouper(NewRayGrouper(client, defaultgrouper.NewDefaultGrouper(queueLabelKey, nodePoolLabelKey, client)))
	return grouper.GetPodGroupMetadata(rayCluster, pod)
}

func subGroupByName(subGroups []*podgroup.SubGroupMetadata, name string) *podgroup.SubGroupMetadata {
	for _, subGroup := range subGroups {
		if subGroup.Name == name {
			return subGroup
		}
	}
	return nil
}

func TestRaySegments_SubGroupTree(t *testing.T) {
	rayCluster := segmentedRayCluster(
		map[string]interface{}{constants.TopologyKey: "cluster-topology"},
		segmentedWorkerGroup(8, 6, map[string]interface{}{
			constants.SegmentSizeKey:                       "4",
			constants.SegmentTopologyRequiredPlacementKey:  "rack",
			constants.SegmentTopologyPreferredPlacementKey: "node",
		}),
	)
	pod := rayWorkerPod("worker-5", map[string]string{utils.RayWorkerReplicaIndexKey: "5"})

	metadata, err := getSegmentedMetadata(t, rayCluster, pod)
	require.NoError(t, err)

	// Segmentation doesn't change the gang size: 1 (head) + 6 (minReplicas)
	assert.Equal(t, int32(7), metadata.MinAvailable)
	require.Len(t, metadata.SubGroups, 4)

	parent := subGroupByName(metadata.SubGroups, "workers")
	require.NotNil(t, parent)
	assert.Equal(t, ptr.To(int32(2)), parent.MinSubGroup)
	assert.Nil(t, parent.Parent)
	assert.Empty(t, parent.PodsReferences)

	expectedConstraints := &podgroup.TopologyConstraintMetadata{
		Topology:               "cluster-topology",
		RequiredTopologyLevel:  "rack",
		PreferredTopologyLevel: "node",
	}
	segment0 := subGroupByName(metadata.SubGroups, "workers-segment-0")
	require.NotNil(t, segment0)
	assert.Equal(t, ptr.To("workers"), segment0.Parent)
	assert.Equal(t, int32(4), segment0.MinAvailable)
	assert.Equal(t, expectedConstraints, segment0.TopologyConstraints)
	assert.Empty(t, segment0.PodsReferences)

	// minReplicas 6 = 4 in segment-0 + 2 in segment-1
	segment1 := subGroupByName(metadata.SubGroups, "workers-segment-1")
	require.NotNil(t, segment1)
	assert.Equal(t, int32(2), segment1.MinAvailable)
	assert.Equal(t, expectedConstraints, segment1.TopologyConstraints)
	assert.Equal(t, []string{"worker-5"}, segment1.PodsReferences)
}

func TestRaySegments_PartialAndElasticSegments(t *testing.T) {
	// 7 pods in segments of 3 -> sizes 3, 3, 1; minReplicas 2 -> only segment-0 is mandatory.
	rayCluster := segmentedRayCluster(nil, segmentedWorkerGroup(7, 2, map[string]interface{}{
		constants.SegmentSizeKey: "3",
	}))
	pod := rayWorkerPod("worker-6", map[string]string{utils.RayWorkerReplicaIndexKey: "6"})

	metadata, err := getSegmentedMetadata(t, rayCluster, pod)
	require.NoError(t, err)

	assert.Equal(t, ptr.To(int32(3)), subGroupByName(metadata.SubGroups, "workers").MinSubGroup)
	assert.Equal(t, int32(2), subGroupByName(metadata.SubGroups, "workers-segment-0").MinAvailable)
	assert.Equal(t, int32(0), subGroupByName(metadata.SubGroups, "workers-segment-1").MinAvailable)
	segment2 := subGroupByName(metadata.SubGroups, "workers-segment-2")
	assert.Equal(t, int32(0), segment2.MinAvailable)
	assert.Equal(t, []string{"worker-6"}, segment2.PodsReferences)
	// No segment placement annotations -> segments without topology constraints
	assert.Nil(t, segment2.TopologyConstraints)
}

func TestRaySegments_MultiHostGroup(t *testing.T) {
	workerGroup := segmentedWorkerGroup(3, 3, map[string]interface{}{constants.SegmentSizeKey: "4"})
	workerGroup["numOfHosts"] = int64(2)
	rayCluster := segmentedRayCluster(nil, workerGroup)

	for _, tc := range []struct {
		replicaIndex, hostIndex string
		expectedSegment         string
	}{
		{"0", "0", "workers-segment-0"},
		{"1", "1", "workers-segment-0"},
		{"2", "0", "workers-segment-1"},
		{"2", "1", "workers-segment-1"},
	} {
		pod := rayWorkerPod("worker", map[string]string{
			utils.RayWorkerReplicaIndexKey: tc.replicaIndex,
			utils.RayHostIndexKey:          tc.hostIndex,
		})
		metadata, err := getSegmentedMetadata(t, rayCluster, pod)
		require.NoError(t, err)

		// 3 replicas x 2 hosts = 6 pods -> segments of 4 and 2
		assert.Equal(t, int32(4), subGroupByName(metadata.SubGroups, "workers-segment-0").MinAvailable)
		assert.Equal(t, int32(2), subGroupByName(metadata.SubGroups, "workers-segment-1").MinAvailable)
		assert.Equal(t, []string{"worker"}, subGroupByName(metadata.SubGroups, tc.expectedSegment).PodsReferences,
			"replica %s host %s", tc.replicaIndex, tc.hostIndex)
	}
}

func TestRaySegments_IndexBeyondReplicasGoesToLastSegment(t *testing.T) {
	rayCluster := segmentedRayCluster(nil, segmentedWorkerGroup(4, 4, map[string]interface{}{constants.SegmentSizeKey: "2"}))
	pod := rayWorkerPod("survivor", map[string]string{utils.RayWorkerReplicaIndexKey: "7"})

	metadata, err := getSegmentedMetadata(t, rayCluster, pod)
	require.NoError(t, err)
	assert.Equal(t, []string{"survivor"}, subGroupByName(metadata.SubGroups, "workers-segment-1").PodsReferences)
}

func TestRaySegments_HeadPodIsNotSegmented(t *testing.T) {
	rayCluster := segmentedRayCluster(nil, segmentedWorkerGroup(4, 4, map[string]interface{}{constants.SegmentSizeKey: "2"}))
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "head", Labels: map[string]string{
		utils.RayNodeGroupLabelKey: utils.RayNodeHeadGroupLabelValue,
	}}}

	metadata, err := getSegmentedMetadata(t, rayCluster, pod)
	require.NoError(t, err)
	assert.Equal(t, []string{"head"}, subGroupByName(metadata.SubGroups, utils.RayNodeHeadGroupLabelValue).PodsReferences)
}

func TestRaySegments_TopologyFromTopOwner(t *testing.T) {
	templateAnnotations := map[string]interface{}{
		constants.SegmentSizeKey:                      "2",
		constants.SegmentTopologyRequiredPlacementKey: "rack",
	}
	rayCluster := segmentedRayCluster(nil, segmentedWorkerGroup(4, 4, templateAnnotations))
	rayJob := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind":       "RayJob",
		"apiVersion": "ray.io/v1",
		"metadata": map[string]interface{}{
			"name": "job", "namespace": "test_namespace", "uid": "2",
			"annotations": map[string]interface{}{constants.TopologyKey: "job-topology"},
		},
	}}

	_, subGroups, _, err := calcJobNumOfPodsAndSubGroups(rayCluster, rayJob)
	require.NoError(t, err)
	assert.Equal(t, "job-topology", subGroupByName(subGroups, "workers-segment-0").TopologyConstraints.Topology)
}

func TestRaySegments_PlacementWithoutTopologyIsIgnored(t *testing.T) {
	rayCluster := segmentedRayCluster(nil, segmentedWorkerGroup(4, 4, map[string]interface{}{
		constants.SegmentSizeKey:                      "2",
		constants.SegmentTopologyRequiredPlacementKey: "rack",
	}))
	pod := rayWorkerPod("worker-0", map[string]string{utils.RayWorkerReplicaIndexKey: "0"})

	metadata, err := getSegmentedMetadata(t, rayCluster, pod)
	require.NoError(t, err)
	segment0 := subGroupByName(metadata.SubGroups, "workers-segment-0")
	require.NotNil(t, segment0)
	assert.Nil(t, segment0.TopologyConstraints)
}

func TestRaySegments_Errors(t *testing.T) {
	indexedPod := rayWorkerPod("worker-0", map[string]string{utils.RayWorkerReplicaIndexKey: "0"})
	for name, tc := range map[string]struct {
		templateAnnotations map[string]interface{}
		pod                 *v1.Pod
		errorContains       string
	}{
		"missing replica index label": {
			templateAnnotations: map[string]interface{}{constants.SegmentSizeKey: "2"},
			pod:                 rayWorkerPod("worker", nil),
			errorContains:       "RayMultiHostIndexing",
		},
		"invalid segment size": {
			templateAnnotations: map[string]interface{}{constants.SegmentSizeKey: "0"},
			pod:                 indexedPod,
			errorContains:       "must be a positive integer",
		},
	} {
		t.Run(name, func(t *testing.T) {
			rayCluster := segmentedRayCluster(nil, segmentedWorkerGroup(4, 4, tc.templateAnnotations))
			_, err := getSegmentedMetadata(t, rayCluster, tc.pod)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errorContains)
		})
	}
}
