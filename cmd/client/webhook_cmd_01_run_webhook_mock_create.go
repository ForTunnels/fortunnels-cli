// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/fortunnels/client/internal/webhookapi"
)

func runWebhookMockCreate(args []string) int {
	fs := flag.NewFlagSet("webhook mock create", flag.ExitOnError)
	var (
		common  webhookFlags
		ep      = fs.String("endpoint", "", "endpoint id")
		method  = fs.String("method", "GET", "method to match")
		path    = fs.String("path", "/", "path to match")
		status  = fs.Int("status", 200, "response status")
		body    = fs.String("body", "", "response body")
		delayMS = fs.Int("delay-ms", 0, "response delay")
		maxUses = fs.Int("max-uses", 0, "max uses (0 = unlimited)")
	)
	common.register(fs)
	if err := fs.Parse(args); err != nil {
		return exitError
	}
	if *ep == "" {
		fmt.Fprintln(os.Stderr, "usage: fortunnels webhook mock create --endpoint <id> [flags]")
		return exitError
	}
	client, err := common.client()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return exitError
	}
	var maxUsesPtr *int
	if *maxUses > 0 {
		maxUsesPtr = intPtr(*maxUses)
	}
	rule, err := client.CreateMock(context.Background(), *ep, &webhookapi.UpsertStubRuleRequest{
		Method: *method, Path: *path,
		Response: webhookapi.MockResp{Status: *status, Body: *body, DelayMS: *delayMS},
		MaxUses:  maxUsesPtr,
	})
	if err != nil {
		return webhookErrorExit("mock create failed", err)
	}
	fmt.Printf("created rule=%s\n", rule.ID)
	return exitOK
}

func runWebhookMockUpdate(args []string) int {
	fs := flag.NewFlagSet("webhook mock update", flag.ExitOnError)
	var (
		common webhookFlags
		ep     = fs.String("endpoint", "", "endpoint id")
		ruleID = fs.String("rule", "", "rule id")
	)
	changes := registerWebhookMockUpdateFlags(fs)
	common.register(fs)
	if err := fs.Parse(args); err != nil {
		return exitError
	}
	if *ep == "" || *ruleID == "" {
		fmt.Fprintln(os.Stderr, "usage: fortunnels webhook mock update --endpoint <id> --rule <id> [flags]")
		return exitError
	}
	client, err := common.client()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return exitError
	}
	rules, err := client.ListMock(context.Background(), *ep)
	if err != nil {
		return webhookErrorExit("mock list failed", err)
	}
	var existing *webhookapi.StubRule
	for i := range rules {
		if rules[i].ID == *ruleID {
			existing = &rules[i]
			break
		}
	}
	if existing == nil {
		fmt.Fprintln(os.Stderr, "rule not found")
		return exitError
	}
	input := changes.apply(fs, existing)
	rule, err := client.UpdateMock(context.Background(), *ep, *ruleID, input)
	if err != nil {
		return webhookErrorExit("mock update failed", err)
	}
	fmt.Printf("updated rule=%s version=%d\n", rule.ID, rule.Version)
	return exitOK
}

type webhookMockUpdateChanges struct {
	method, path     string
	response         webhookapi.MockResp
	version, maxUses int
}

func registerWebhookMockUpdateFlags(fs *flag.FlagSet) *webhookMockUpdateChanges {
	changes := &webhookMockUpdateChanges{}
	fs.StringVar(&changes.method, "method", "", "method to match")
	fs.StringVar(&changes.path, "path", "", "path to match")
	fs.IntVar(&changes.response.Status, "status", 0, "response status")
	fs.StringVar(&changes.response.Body, "body", "", "response body")
	fs.IntVar(&changes.response.DelayMS, "delay-ms", -1, "response delay")
	fs.IntVar(&changes.version, "version", 0, "expected rule version")
	fs.IntVar(&changes.maxUses, "max-uses", 0, "max uses (0 = keep current)")
	return changes
}

func (changes *webhookMockUpdateChanges) apply(fs *flag.FlagSet, existing *webhookapi.StubRule) *webhookapi.UpsertStubRuleRequest {
	enabled := existing.Enabled
	input := &webhookapi.UpsertStubRuleRequest{
		Priority: existing.Priority,
		Enabled:  &enabled,
		Method:   existing.Method,
		PathMode: existing.PathMode,
		Path:     existing.Path,
		Response: existing.Response,
		MaxUses:  existing.MaxUses,
		Version:  existing.Version,
	}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "method":
			input.Method = changes.method
		case "path":
			input.Path = changes.path
		case "status":
			input.Response.Status = changes.response.Status
		case "body":
			input.Response.Body = changes.response.Body
		case "delay-ms":
			input.Response.DelayMS = changes.response.DelayMS
		case "version":
			input.Version = changes.version
		case "max-uses":
			if changes.maxUses > 0 {
				input.MaxUses = intPtr(changes.maxUses)
			}
		}
	})
	return input
}

func runWebhookMockDelete(args []string) int {
	fs := flag.NewFlagSet("webhook mock delete", flag.ExitOnError)
	var (
		common webhookFlags
		ep     = fs.String("endpoint", "", "endpoint id")
		ruleID = fs.String("rule", "", "rule id")
	)
	common.register(fs)
	if err := fs.Parse(args); err != nil {
		return exitError
	}
	if *ep == "" || *ruleID == "" {
		fmt.Fprintln(os.Stderr, "usage: fortunnels webhook mock delete --endpoint <id> --rule <id>")
		return exitError
	}
	client, err := common.client()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return exitError
	}
	if err := client.DeleteMock(context.Background(), *ep, *ruleID); err != nil {
		return webhookErrorExit("mock delete failed", err)
	}
	fmt.Printf("deleted rule=%s\n", *ruleID)
	return exitOK
}

// multiFlag collects repeated string flags.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(value string) error {
	*m = append(*m, value)
	return nil
}

func intPtr(v int) *int { return &v }
