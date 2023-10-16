package mpt

//go:generate mockgen -source visitor.go -destination visitor_mocks.go -package mpt

import (
	"fmt"
	"strings"
)

// ----------------------------------------------------------------------------
//                            Visitor Interface
// ----------------------------------------------------------------------------

// NodeVisitor defines an interface for any consumer interested in visiting a
// set of nodes of a Forest. It is intended for facilitating generic trie and
// forest analysis infrastructure.
type NodeVisitor interface {
	// Visit is called for each node. Through the response the visitor can
	// decide whether more nodes should be visited, the visiting should be
	// aborted or (in some situations) whether the child nodes of the current
	// node should be skipped.
	Visit(Node, NodeInfo) VisitResponse
}

type NodeInfo struct {
	Id    NodeId // the ID of the visited node
	Depth *int   // the nesting level of the visited node, only set for tree visits
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
	defer source.close()
	return source.forAllNodes(func(id NodeId, node Node) error {
		visitor.Visit(node, NodeInfo{Id: id})
		return nil
	})
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
		if !child.IsEmpty() {
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
