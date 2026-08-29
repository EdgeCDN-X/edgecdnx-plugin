package edgecdnxplugin

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"

	kruntime "k8s.io/apimachinery/pkg/runtime"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/coredns/coredns/plugin/pkg/log"
)

type NSRecord struct {
	Name string
	IPv4 string
	IPv6 string
}

func parseResponseType(raw string) (ResponseType, error) {
	value := ResponseType(strings.ToUpper(raw))
	switch value {
	case CNAME, A_AAAA:
		return value, nil
	default:
		return "", fmt.Errorf("invalid response type %q, expected one of: %s, %s", raw, CNAME, A_AAAA)
	}
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

	var namespace, soa string // Base Zone configuration
	var ns []NSRecord = make([]NSRecord, 0)
	var recordttl uint32 = 60
	var staticDefaultWeight int32 = 100
	var routingMode = RoutingModeAdaptive
	var dnsResponseType ResponseType = A_AAAA
	var grpcResponseType ResponseType = CNAME

	for c.NextBlock() {
		val := c.Val()
		args := c.RemainingArgs()
		if val == "namespace" {
			namespace = args[0]
		}
		if val == "soa" {
			soa = args[0]
		}
		if val == "ns" {
			if len(args) != 2 {
				return plugin.Error("edgecdnx", fmt.Errorf("expected 2 arguments for ns, got %d", len(args)))
			}
			// TODO support for IPv6
			ns = append(ns, NSRecord{Name: args[0], IPv4: args[1]})
		}
		if val == "recordttl" {
			raw, err := strconv.Atoi(args[0])
			if err != nil {
				return plugin.Error("edgecdnx", fmt.Errorf("failed to parse recordttl: %w", err))
			}
			recordttl = uint32(raw)
		}
		if val == "defaultweight" {
			if len(args) != 1 {
				return plugin.Error("edgecdnx", fmt.Errorf("expected 1 argument for defaultweight, got %d", len(args)))
			}
			raw, err := strconv.Atoi(args[0])
			if err != nil || raw < 1 || raw > 100 {
				return plugin.Error("edgecdnx", fmt.Errorf("defaultweight must be an integer within [1,100]"))
			}
			staticDefaultWeight = int32(raw)
		}
		if val == "routingmode" {
			if len(args) != 1 || (args[0] != RoutingModeAdaptive && args[0] != RoutingModeDeterministic && args[0] != RoutingModeStaticRendezvous) {
				return plugin.Error("edgecdnx", fmt.Errorf("routingmode must be one of: %s, %s, %s", RoutingModeAdaptive, RoutingModeDeterministic, RoutingModeStaticRendezvous))
			}
			routingMode = args[0]
		}
		if val == "dnsresponsetype" {
			if len(args) != 1 {
				return plugin.Error("edgecdnx", fmt.Errorf("expected 1 argument for dnsresponsetype, got %d", len(args)))
			}
			parsed, err := parseResponseType(args[0])
			if err != nil {
				return plugin.Error("edgecdnx", err)
			}
			dnsResponseType = parsed
		}
		if val == "grpcresponsetype" {
			if len(args) != 1 {
				return plugin.Error("edgecdnx", fmt.Errorf("expected 1 argument for grpcresponsetype, got %d", len(args)))
			}
			parsed, err := parseResponseType(args[0])
			if err != nil {
				return plugin.Error("edgecdnx", err)
			}
			grpcResponseType = parsed
		}
	}

	clientSet, err := dynamic.NewForConfig(kubeconfig)
	if err != nil {
		return plugin.Error("edgecdnx", fmt.Errorf("failed to create dynamic client: %w", err))
	}

	fac := dynamicinformer.NewFilteredDynamicSharedInformerFactory(clientSet, 10*time.Minute, namespace, nil)
	qualityEnabled := true
	preflightContext, cancelPreflight := context.WithTimeout(context.Background(), 5*time.Second)
	_, qualityErr := clientSet.Resource(nodeQualityGVR).Namespace(namespace).List(preflightContext, metav1.ListOptions{Limit: 1})
	cancelPreflight()
	if apierrors.IsNotFound(qualityErr) {
		qualityEnabled = false
		log.Warning("edgeroute: NodeQuality CRD is not installed; using static EdgeCDN-X routing (fail-open)")
	} else if qualityErr != nil {
		return plugin.Error("edgecdnx", fmt.Errorf("NodeQuality API preflight failed: %w", qualityErr))
	}
	nodeQualityManager := NewNodeQualityManager(fac, qualityEnabled)

	zoneManager := NewZoneManager(fac, ZoneManagerConfiguration{
		Namespace: namespace,
		SOA:       soa,
		NS:        ns,
		Origins:   origins,
	})

	serviceManager := NewServiceManager(fac, ServiceManagerConfiguration{
		Namespace: namespace,
	})

	prefixListRoutingManager := NewPrefixListRoutingManager(fac, PrefixListRoutingManagerConfiguration{
		Namespace: namespace,
	})

	locationManager := NewLocationManager(fac, LocationManagerConfiguration{
		Namespace:           namespace,
		RecrodTTL:           recordttl,
		StaticDefaultWeight: staticDefaultWeight,
		RoutingMode:         routingMode,
	}, nodeQualityManager)

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
			ServiceManager:           serviceManager,
			PrefixListRoutingManager: prefixListRoutingManager,
			LocationManager:          locationManager,
			NodeQualityManager:       nodeQualityManager,
			DNSResponseType:          dnsResponseType,
			GRPCResponseType:         grpcResponseType,
		}
	})

	return nil
}
