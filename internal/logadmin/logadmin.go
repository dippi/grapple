// Copyright 2016 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// These features are missing now, but will likely be added:
// - There is no way to specify CallOptions.

// Package logadmin contains a Cloud Logging client that can be used
// for reading logs and working with sinks, metrics and monitored resources.
// For a client that can write logs, see package cloud.google.com/go/logging.
//
// The client uses Logging API v2.
// See https://cloud.google.com/logging/docs/api/v2/ for an introduction to the API.
//
// Note: This package is in beta.  Some backwards-incompatible changes may occur.
package logadmin // import "cloud.google.com/go/logging/logadmin"

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	vkit "cloud.google.com/go/logging/apiv2"
	logpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	_ "google.golang.org/genproto/googleapis/appengine/logging/v1" // Import the following so EntryIterator can unmarshal log protos.
	_ "google.golang.org/genproto/googleapis/cloud/audit"
)

// Version is the current tagged release of the library.
const Version = "1.13.0"

// Client is a Logging client. A Client is associated with a single Cloud project.
type Client struct {
	lClient *vkit.Client        // logging client
	parent  string
	closed  bool
}

// NewClient returns a new logging client associated with the provided project ID.
//
// By default NewClient uses AdminScope. To use a different scope, call
// NewClient using a WithScopes option (see https://godoc.org/google.golang.org/api/option#WithScopes).
func NewClient(ctx context.Context, parent string, opts ...option.ClientOption) (*Client, error) {
	if !strings.ContainsRune(parent, '/') {
		parent = "projects/" + parent
	}
	opts = append([]option.ClientOption{
		option.WithScopes(logging.AdminScope),
	}, opts...)
	lc, err := vkit.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	lc.SetGoogleClientInfo("gccl", Version)
	client := &Client{
		lClient: lc,
		parent:  parent,
	}
	return client, nil
}

// Close closes the client.
func (c *Client) Close() error {
	if c.closed {
		return nil
	}
	// Return only the first error. Since all clients share an underlying connection,
	// Closes after the first always report a "connection is closing" error.
	err := c.lClient.Close()
	c.closed = true
	return err
}

// An EntriesOption is an option for listing log entries.
type EntriesOption interface {
	set(*logpb.ListLogEntriesRequest)
}

// ProjectIDs sets the project IDs or project numbers from which to retrieve
// log entries. Examples of a project ID: "my-project-1A", "1234567890".
func ProjectIDs(pids []string) EntriesOption { return projectIDs(pids) }

type projectIDs []string

func (p projectIDs) set(r *logpb.ListLogEntriesRequest) {
	r.ResourceNames = make([]string, len(p))
	for i, v := range p {
		r.ResourceNames[i] = fmt.Sprintf("projects/%s", v)
	}
}

// ResourceNames sets the resource names from which to retrieve
// log entries. Examples: "projects/my-project-1A", "organizations/my-org".
func ResourceNames(rns []string) EntriesOption { return resourceNames(rns) }

type resourceNames []string

func (rn resourceNames) set(r *logpb.ListLogEntriesRequest) {
	r.ResourceNames = append([]string(nil), rn...)
}

// Filter sets an advanced logs filter for listing log entries (see
// https://cloud.google.com/logging/docs/view/advanced_filters). The filter is
// compared against all log entries in the projects specified by ProjectIDs.
// Only entries that match the filter are retrieved. An empty filter (the
// default) matches all log entries.
//
// In the filter string, log names must be written in their full form, as
// "projects/PROJECT-ID/logs/LOG-ID". Forward slashes in LOG-ID must be
// replaced by %2F before calling Filter.
//
// Timestamps in the filter string must be written in RFC 3339 format. By default,
// timestamp filters for the past 24 hours.
func Filter(f string) EntriesOption { return filter(f) }

type filter string

func (f filter) set(r *logpb.ListLogEntriesRequest) { r.Filter = string(f) }

// NewestFirst causes log entries to be listed from most recent (newest) to
// least recent (oldest). By default, they are listed from oldest to newest.
func NewestFirst() EntriesOption { return newestFirst{} }

type newestFirst struct{}

func (newestFirst) set(r *logpb.ListLogEntriesRequest) { r.OrderBy = "timestamp desc" }

// PageSize provide a way to override number of results to return from each request.
func PageSize(p int32) EntriesOption { return pageSize(p) }

type pageSize int32

func (p pageSize) set(r *logpb.ListLogEntriesRequest) { r.PageSize = int32(p) }

// Entries returns an EntryIterator for iterating over log entries. By default,
// the log entries will be restricted to those from the project passed to
// NewClient. This may be overridden by passing a ProjectIDs option. Requires ReadScope or AdminScope.
func (c *Client) Entries(ctx context.Context, opts ...EntriesOption) *EntryIterator {
	it := &EntryIterator{
		it: c.lClient.ListLogEntries(ctx, listLogEntriesRequest(c.parent, opts)),
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(
		it.fetch,
		func() int { return len(it.items) },
		func() interface{} { b := it.items; it.items = nil; return b })
	return it
}

func listLogEntriesRequest(parent string, opts []EntriesOption) *logpb.ListLogEntriesRequest {
	req := &logpb.ListLogEntriesRequest{
		ResourceNames: []string{parent},
	}
	for _, opt := range opts {
		opt.set(req)
	}
	req.Filter = defaultTimestampFilter(req.Filter)
	return req
}

// defaultTimestampFilter returns a timestamp filter that looks back 24 hours in the past.
// This default setting is consistent with documentation. Note: user filters containing 'timestamp'
// substring disables this default timestamp filter, e.g. `textPayload: "timestamp"`
func defaultTimestampFilter(filter string) string {
	dayAgo := time.Now().Add(-24 * time.Hour).UTC()
	switch {
	case len(filter) == 0:
		return fmt.Sprintf(`timestamp >= "%s"`, dayAgo.Format(time.RFC3339))
	case !strings.Contains(strings.ToLower(filter), "timestamp"):
		return fmt.Sprintf(`%s AND timestamp >= "%s"`, filter, dayAgo.Format(time.RFC3339))
	default:
		return filter
	}
}

// An EntryIterator iterates over log entries.
type EntryIterator struct {
	it       *vkit.LogEntryIterator
	pageInfo *iterator.PageInfo
	nextFunc func() error
	items    []*logpb.LogEntry
}

// PageInfo supports pagination. See https://godoc.org/google.golang.org/api/iterator package for details.
func (it *EntryIterator) PageInfo() *iterator.PageInfo { return it.pageInfo }

// Next returns the next result. Its second return value is iterator.Done
// (https://godoc.org/google.golang.org/api/iterator) if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *EntryIterator) Next() (*logpb.LogEntry, error) {
	if err := it.nextFunc(); err != nil {
		return nil, err
	}
	item := it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *EntryIterator) fetch(pageSize int, pageToken string) (string, error) {
	return iterFetch(pageSize, pageToken, it.it.PageInfo(), func() error {
		item, err := it.it.Next()
		if err != nil {
			return err
		}
		it.items = append(it.items, item)
		return nil
	})
}

// Common fetch code for iterators that are backed by vkit iterators.
func iterFetch(pageSize int, pageToken string, pi *iterator.PageInfo, next func() error) (string, error) {
	pi.MaxSize = pageSize
	pi.Token = pageToken
	// Get one item, which will fill the buffer.
	if err := next(); err != nil {
		return "", err
	}
	// Collect the rest of the buffer.
	for pi.Remaining() > 0 {
		if err := next(); err != nil {
			return "", err
		}
	}
	return pi.Token, nil
}
