// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

//go:generate mockgen -source visitor.go -destination visitor_mocks.go -package mpt

import (
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/common/tribool"
	"strings"
)

// ----------------------------------------------------------------------------
//                            Visitor Interface
// ----------------------------------------------------------------------------

// NodeVisitor defines an interface for any consumer interested in visiting a
// set of nodes of a Forest. It is intended for generic trie and
// forest analysis infrastructure.
type NodeVisitor interface {
	// Visit is called for each node. Through the response the visitor can
	// decide control the visiting process. It may be
	//  - continued: keep processing additional nodes
	//  - aborted: stop processing nodes and end node iteration
	//  - pruned: skip the child nodes of the current node and continue with
	//       the next node following the last descendent of the current node
	// While the first two options are supported in trees and forests, the
	// last one is only supported on trees.
	Visit(Node, NodeInfo) VisitResponse
}

type NodeInfo struct {
	Id       NodeId          // the ID of the visited node
	Depth    *int            // the nesting level of the visited node, only set for tree visits
	Embedded tribool.Tribool // true if this node is embedded in another node, tracked in visitPathTo
}

type VisitResponse int

const (
	VisitResponseContinue VisitResponse = 0
	VisitResponseAbort    VisitResponse = 1
	VisitResponsePrune    VisitResponse = 2
)

// VisitForestNodes load the nodes of the forest stored in the given directory and
// applies the visitor on each of those.
func VisitForestNodes(directory string, config MptConfig, visitor NodeVisitor) error {
	source, err := openVerificationNodeSource(directory, config)
	if err != nil {
		return err
	}
	defer source.Close()
	return source.forAllNodes(func(id NodeId, node Node) error {
		visitor.Visit(node, NodeInfo{Id: id})
		return nil
	})
}

// ----------------------------------------------------------------------------
//                          Lambda Visitor
// ----------------------------------------------------------------------------

// MakeVisitor wraps a function into the node visitor interface.
func MakeVisitor(visit func(Node, NodeInfo) VisitResponse) NodeVisitor {
	return &lambdaVisitor{visit}
}

type lambdaVisitor struct {
	visit func(Node, NodeInfo) VisitResponse
}

func (v *lambdaVisitor) Visit(n Node, i NodeInfo) VisitResponse {
	return v.visit(n, i)
}

// ----------------------------------------------------------------------------
//                            Node Statistics
// ----------------------------------------------------------------------------

// GetTrieNodeStatistics computes node statistics for the given Trie.
func GetTrieNodeStatistics(trie *LiveTrie) (NodeStatistic, error) {
	collector := &nodeStatisticsCollector{}
	if err := trie.VisitTrie(collector); err != nil {
		return NodeStatistic{}, err
	}
	return collector.stats, nil
}

// GetForestNodeStatistics computes node statistics for the MPT forest stored in
// the given directory.
func GetForestNodeStatistics(directory string, config MptConfig) (NodeStatistic, error) {
	collector := &nodeStatisticsCollector{}
	if err := VisitForestNodes(directory, config, collector); err != nil {
		return NodeStatistic{}, err
	}
	return collector.stats, nil
}

type NodeStatistic struct {
	numBranches   int
	numAccounts   int
	numValues     int
	numExtensions int

	numChildren [17]int

	depths []int
}

func (s *NodeStatistic) String() string {
	builder := strings.Builder{}

	builder.WriteString("Node types:\n")
	builder.WriteString(fmt.Sprintf("Accounts, %d\n", s.numAccounts))
	builder.WriteString(fmt.Sprintf("Branches, %d\n", s.numBranches))
	builder.WriteString(fmt.Sprintf("Extensions, %d\n", s.numExtensions))
	builder.WriteString(fmt.Sprintf("Values, %d\n", s.numValues))

	builder.WriteString("Branch-Node-Size Distribution:\n")
	for i, count := range s.numChildren {
		builder.WriteString(fmt.Sprintf("%d, %d\n", i, count))
	}

	if len(s.depths) > 0 {
		builder.WriteString("Node depth distribution:\n")
		for i, count := range s.depths {
			builder.WriteString(fmt.Sprintf("%d, %d\n", i, count))
		}
	}

	return builder.String()
}

type nodeStatisticsCollector struct {
	stats NodeStatistic
}

func (c *nodeStatisticsCollector) Visit(node Node, info NodeInfo) VisitResponse {
	c.registerDepth(info)
	switch t := node.(type) {
	case *AccountNode:
		c.stats.numAccounts++
	case *BranchNode:
		c.visitBranch(t, info)
	case *ExtensionNode:
		c.stats.numExtensions++
	case *ValueNode:
		c.stats.numValues++
	}
	return VisitResponseContinue
}

func (c *nodeStatisticsCollector) visitBranch(b *BranchNode, info NodeInfo) {
	c.stats.numBranches++
	numChildren := 0
	for _, child := range b.children {
		if !child.Id().IsEmpty() {
			numChildren++
		}
	}
	c.stats.numChildren[numChildren]++
}

func (c *nodeStatisticsCollector) registerDepth(info NodeInfo) {
	if info.Depth == nil {
		return
	}
	for len(c.stats.depths) <= *info.Depth {
		c.stats.depths = append(c.stats.depths, 0)
	}
	c.stats.depths[*info.Depth]++
}
