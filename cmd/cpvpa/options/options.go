/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

// Package options contains flags for initializing an autoscaler.
package options

import (
	goflag "flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/spf13/pflag"
)

const (
	defaultResizeFallbackGracePeriod     = 5 * time.Minute
	defaultResizeFallbackMaxPodsPerCycle = 1
)

// AutoScalerConfig configures and runs an autoscaler server
type AutoScalerConfig struct {
	Namespace                     string
	Target                        string
	DefaultConfig                 string
	ConfigFile                    string
	PollPeriodSeconds             int
	Kubeconfig                    string
	PrintVer                      bool
	DryRun                        bool
	ResizeMode                    string
	ResizeFallbackGracePeriod     time.Duration
	ResizeFallbackMaxPodsPerCycle int
}

// NewAutoScalerConfig returns a Autoscaler config
func NewAutoScalerConfig() *AutoScalerConfig {
	return &AutoScalerConfig{
		// Defaults.
		Namespace:                     os.Getenv("MY_NAMESPACE"),
		PollPeriodSeconds:             10,
		PrintVer:                      false,
		DryRun:                        false,
		ResizeMode:                    "Recreate",
		ResizeFallbackGracePeriod:     defaultResizeFallbackGracePeriod,
		ResizeFallbackMaxPodsPerCycle: defaultResizeFallbackMaxPodsPerCycle,
	}
}

// AddFlags adds flags to the specified FlagSet.
func (c *AutoScalerConfig) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&c.Target, "target", c.Target, "The target object to scale. Format: deployment/* (not case sensitive).")
	fs.StringVar(&c.Namespace, "namespace", c.Namespace, "The Namespace of the --target. Defaults to ${MY_NAMESPACE}.")
	fs.StringVar(&c.DefaultConfig, "default-config", c.DefaultConfig, "The default configuration (in JSON format).")
	fs.StringVar(&c.ConfigFile, "config-file", c.ConfigFile, "A config file (in JSON format), which overrides the --default-config.")
	fs.IntVar(&c.PollPeriodSeconds, "poll-period-seconds", c.PollPeriodSeconds, "The period, in seconds, to poll cluster size and perform autoscaling.")
	fs.StringVar(&c.Kubeconfig, "kubeconfig", c.Kubeconfig, "Path to a kubeconfig. Only required if running out-of-cluster.")
	fs.BoolVar(&c.PrintVer, "version", c.PrintVer, "Print the version and exit.")
	fs.BoolVar(&c.DryRun, "dry-run", c.PrintVer, "Calulate updates for a target but does not apply the update.")
	fs.StringVar(&c.ResizeMode, "resize-mode", c.ResizeMode, "How to apply resource changes. One of: Recreate, InPlace, InPlaceOrRecreate. Recreate is the legacy behaviour. InPlace requires Kubernetes 1.33+.")
	fs.DurationVar(&c.ResizeFallbackGracePeriod, "resize-fallback-grace-period", c.ResizeFallbackGracePeriod, "Only used with InPlaceOrRecreate. How long a pod must continuously fail to resize (Infeasible, Deferred, or stuck in progress) before cpvpa recreates it so the controller can reschedule it at the new size.")
	fs.IntVar(&c.ResizeFallbackMaxPodsPerCycle, "resize-fallback-max-pods-per-cycle", c.ResizeFallbackMaxPodsPerCycle, "Only used with InPlaceOrRecreate. Caps how many not-yet-resized pods cpvpa recreates (by direct delete) in a single poll cycle.")
}

// InitFlags no// WordSepNormalizeFunc changes all flags that contain "_" separators
func WordSepNormalizeFunc(f *pflag.FlagSet, name string) pflag.NormalizedName {
	if strings.Contains(name, "_") {
		return pflag.NormalizedName(strings.ReplaceAll(name, "_", "-"))
	}
	return pflag.NormalizedName(name)
}

func (c *AutoScalerConfig) InitFlags() {
	pflag.CommandLine.SetNormalizeFunc(WordSepNormalizeFunc)
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()
	_ = goflag.CommandLine.Parse([]string{}) // Hack to stop noisy logs.
}

// ValidateFlags validates whether flags are set up correctly
func (c *AutoScalerConfig) ValidateFlags() error {
	var errorsFound bool

	c.Target = strings.ToLower(c.Target)
	if !isTargetFormatValid(c.Target) {
		errorsFound = true
	}
	if c.Namespace == "" {
		errorsFound = true
		glog.Errorf("--namespace parameter not set and failed to fallback")
	}
	if c.DefaultConfig == "" && c.ConfigFile == "" {
		errorsFound = true
		glog.Errorf("Either --default-config or --config-file must be specified")
	}
	if c.PollPeriodSeconds < 1 {
		errorsFound = true
		glog.Errorf("--poll-period-seconds cannot be less than 1")
	}
	switch c.ResizeMode {
	case "Recreate", "InPlace", "InPlaceOrRecreate":
	default:
		errorsFound = true
		glog.Errorf("--resize-mode must be one of: Recreate, InPlace, InPlaceOrRecreate (got %q)", c.ResizeMode)
	}

	if c.ResizeFallbackGracePeriod <= 0 {
		errorsFound = true
		glog.Errorf("--resize-fallback-grace-period must be positive (got %v)", c.ResizeFallbackGracePeriod)
	}
	if c.ResizeFallbackMaxPodsPerCycle <= 0 {
		errorsFound = true
		glog.Errorf("--resize-fallback-max-pods-per-cycle must be > 0 (got %d)", c.ResizeFallbackMaxPodsPerCycle)
	}
	if c.ResizeMode != "InPlaceOrRecreate" {
		if c.ResizeFallbackGracePeriod != defaultResizeFallbackGracePeriod || c.ResizeFallbackMaxPodsPerCycle != defaultResizeFallbackMaxPodsPerCycle {
			glog.Warningf("--resize-fallback-grace-period and --resize-fallback-max-pods-per-cycle are ignored when --resize-mode=%q", c.ResizeMode)
		}
	}

	// Log all sanity check errors before returning a single error string
	if errorsFound {
		return fmt.Errorf("failed to validate config parameters")
	}
	return nil
}

func isTargetFormatValid(target string) bool {
	if target == "" {
		glog.Errorf("--target parameter cannot be empty")
		return false
	}
	target = strings.ToLower(target)

	if strings.HasPrefix(target, "deployment/") ||
		strings.HasPrefix(target, "daemonset/") ||
		strings.HasPrefix(target, "replicaset/") {
		return true
	}

	glog.Errorf("Unknown target format: must be one of deployment/*, daemonset/*, or replicaset/* (not case sensitive).")
	return false
}
