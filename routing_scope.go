package edgecdnxplugin

import (
	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

type routingScope struct {
	namespace string
	project   string
}

func scopeFor(namespace string, project *corev1.LocalObjectReference) routingScope {
	scope := routingScope{namespace: namespace}
	if project != nil {
		scope.project = project.Name
	}
	return scope
}

func projectName(project *corev1.LocalObjectReference) string {
	if project == nil {
		return ""
	}
	return project.Name
}

func locationAvailableToScope(locationNamespace, locationProject string, sharing *infrastructurev1alpha1.ResourceSharing, scopes []routingScope) bool {
	if len(scopes) == 0 {
		return true
	}
	source := scopes[0]
	if source.namespace == "" && source.project == "" && locationNamespace == "" && locationProject == "" {
		return true
	}
	return infrastructurev1alpha1.CanUseResource(source.namespace, source.project, locationNamespace, locationProject, sharing)
}

func matchesRoutingScope(namespace string, project *corev1.LocalObjectReference, scopes []routingScope) bool {
	return len(scopes) == 0 || scopes[0] == scopeFor(namespace, project)
}
