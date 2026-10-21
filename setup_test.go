package edgecdnxplugin

import (
	"context"
	"testing"
	"time"

	api "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/tools/cache"
)

func TestParseNamespace(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		valid bool
	}{
		{"explicit namespace", []string{"edgecdnx"}, true},
		{"missing argument", nil, false},
		{"empty argument", []string{""}, false},
		{"multiple arguments", []string{"one", "two"}, false},
		{"invalid namespace", []string{"Invalid/namespace"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseNamespace(test.args)
			if (err == nil) != test.valid {
				t.Fatalf("parseNamespace() = %q, %v", got, err)
			}
			if test.valid && got != test.args[0] {
				t.Fatalf("namespace = %q", got)
			}
		})
	}
}

func TestInformerNamespaceSelection(t *testing.T) {
	for _, namespace := range []string{metav1.NamespaceAll, "tenant-a"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := api.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			objects := make([]runtime.Object, 0, 8)
			for _, tenant := range []string{"tenant-a", "tenant-b"} {
				metadata := metav1.ObjectMeta{Name: "same-name", Namespace: tenant}
				objects = append(objects,
					&api.Location{ObjectMeta: metadata},
					&api.PrefixList{ObjectMeta: metadata},
					&api.Zone{ObjectMeta: metadata, Spec: api.ZoneSpec{Zone: tenant + ".example.com", Email: "noc.example.com."}},
					&api.DNSEndpoint{ObjectMeta: metadata, Spec: api.DNSEndpointSpec{DNSName: tenant + ".example.com", RecordType: "A"}},
				)
			}
			client := fake.NewSimpleDynamicClient(scheme, objects...)
			factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, time.Hour, namespace, nil)
			locations := NewLocationManager(factory, LocationManagerConfiguration{Namespace: namespace})
			prefixes := NewPrefixListRoutingManager(factory, PrefixListRoutingManagerConfiguration{Namespace: namespace})
			zones := NewZoneManager(factory, ZoneManagerConfiguration{Namespace: namespace, Origins: []string{"."}, SOA: "ns.example.com."})
			endpoints := NewDNSEndpointManager(factory, DNSEndpointManagerConfiguration{Namespace: namespace})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			factory.Start(ctx.Done())
			for resource, synced := range factory.WaitForCacheSync(ctx.Done()) {
				if !synced {
					t.Fatalf("%s informer did not sync", resource)
				}
			}
			want := 1
			if namespace == metav1.NamespaceAll {
				want = 2
			}
			for _, informer := range []cache.SharedIndexInformer{locations.Informer, prefixes.Informer, zones.Informer, endpoints.Informer} {
				if got := len(informer.GetStore().List()); got != want {
					t.Fatalf("informer watched %d resources, want %d", got, want)
				}
			}
			for _, action := range client.Actions() {
				if (action.GetVerb() == "list" || action.GetVerb() == "watch") && action.GetNamespace() != namespace {
					t.Fatalf("%s %s used namespace %q, want %q", action.GetVerb(), action.GetResource(), action.GetNamespace(), namespace)
				}
			}
			cancel()
			factory.Shutdown()
		})
	}
}
