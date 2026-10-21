package edgecdnxplugin

import corev1 "k8s.io/api/core/v1"

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

func matchesRoutingScope(namespace string, project *corev1.LocalObjectReference, scopes []routingScope) bool {
	return len(scopes) == 0 || scopes[0] == scopeFor(namespace, project)
}
