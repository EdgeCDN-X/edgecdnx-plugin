package edgecdnxplugin

import (
	"fmt"
	"strconv"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	kruntime "k8s.io/apimachinery/pkg/runtime"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/coredns/coredns/plugin/pkg/log"
)

type NSRecord struct {
	Target string
}

// init registers this plugin.
func init() { plugin.Register("edgecdnx", setup) }

// setup is the function that gets called when the config parser see the token "example". Setup is responsible
// for parsing any extra options the example plugin may have. The first token this function sees is "example".
func setup(c *caddy.Controller) error {
	scheme := kruntime.NewScheme()
	clientsetscheme.AddToScheme(scheme)
	infrastructurev1alpha1.AddToScheme(scheme)

	kubeconfig := ctrl.GetConfigOrDie()
	c.Next() // plugin name

	// Only parse origins from the server block keys
	origins := plugin.OriginsFromArgsOrServerBlock(c.RemainingArgs(), c.ServerBlockKeys)

	// If no origin specified, use default "."
	if len(origins) == 0 {
		origins = []string{"."}
	}

	log.Infof("edgecdnx: Origins: %v", origins)

	namespace := metav1.NamespaceAll
	var soa string
	var ns []NSRecord = make([]NSRecord, 0)
	var recordttl uint32 = 60

	for c.NextBlock() {
		val := c.Val()
		args := c.RemainingArgs()
		if val == "namespace" {
			var err error
			namespace, err = parseNamespace(args)
			if err != nil {
				return plugin.Error("edgecdnx", err)
			}
		}
		if val == "soa" {
			soa = args[0]
		}
		if val == "ns" {
			if len(args) != 1 {
				return plugin.Error("edgecdnx", fmt.Errorf("expected 1 argument for ns, got %d", len(args)))
			}
			ns = append(ns, NSRecord{Target: args[0]})
		}
		if val == "recordttl" {
			raw, err := strconv.Atoi(args[0])
			if err != nil {
				return plugin.Error("edgecdnx", fmt.Errorf("failed to parse recordttl: %w", err))
			}
			recordttl = uint32(raw)
		}
		if val == "dnsresponsetype" {
			if len(args) != 1 {
				return plugin.Error("edgecdnx", fmt.Errorf("expected 1 argument for dnsresponsetype, got %d", len(args)))
			}
			log.Warningf("dnsresponse type argument is deprecated. Router now always returns A or AAAA results")
		}
		if val == "grpcresponsetype" {
			if len(args) != 1 {
				return plugin.Error("edgecdnx", fmt.Errorf("expected 1 argument for grpcresponsetype, got %d", len(args)))
			}
			log.Warningf("grpcresponse type argument is deprecated. Router now always returns A or AAAA results")
		}
	}

	clientSet, err := dynamic.NewForConfig(kubeconfig)
	if err != nil {
		return plugin.Error("edgecdnx", fmt.Errorf("failed to create dynamic client: %w", err))
	}

	fac := dynamicinformer.NewFilteredDynamicSharedInformerFactory(clientSet, 10*time.Minute, namespace, nil)
	if namespace == metav1.NamespaceAll {
		log.Infof("edgecdnx: Watching resources in all namespaces")
	}

	zoneManager := NewZoneManager(fac, ZoneManagerConfiguration{
		Namespace: namespace,
		SOA:       soa,
		NS:        ns,
		Origins:   origins,
	})

	dnsEndpointManager := NewDNSEndpointManager(fac, DNSEndpointManagerConfiguration{
		Namespace: namespace,
	})

	prefixListRoutingManager := NewPrefixListRoutingManager(fac, PrefixListRoutingManagerConfiguration{
		Namespace: namespace,
	})

	locationManager := NewLocationManager(fac, LocationManagerConfiguration{
		Namespace: namespace,
		RecordTTL: recordttl,
	})

	factoryCloseChan := make(chan struct{})
	fac.Start(factoryCloseChan)

	c.OnShutdown(func() error {
		log.Infof("edgecdnx: shutting down informer")
		close(factoryCloseChan)
		fac.Shutdown()
		return nil
	})

	// Add the Plugin to CoreDNS, so Servers can use it in their plugin chain.
	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return EdgeCDNX{
			Next:                     next,
			ZoneManager:              zoneManager,
			DNSEndpointManager:       dnsEndpointManager,
			PrefixListRoutingManager: prefixListRoutingManager,
			LocationManager:          locationManager,
		}
	})

	return nil
}

func parseNamespace(args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("expected 1 argument for namespace, got %d", len(args))
	}
	if problems := validation.IsDNS1123Label(args[0]); len(problems) != 0 {
		return "", fmt.Errorf("invalid namespace %q: %v", args[0], problems)
	}
	return args[0], nil
}
