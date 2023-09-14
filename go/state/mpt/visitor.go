package mpt

import (
	"fmt"
	"strings"
)

type NodeVisitor interface {
	VisitAccount(NodeId, *AccountNode, int)
	VisitBranch(NodeId, *BranchNode, int)
	VisitExtension(NodeId, *ExtensionNode, int)
	VisitValue(NodeId, *ValueNode, int)
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

	builder.WriteString("Number of Branch-Children Distribution:\n")
	for i, count := range s.numChildren {
		builder.WriteString(fmt.Sprintf("%d, %d\n", i, count))
	}

	builder.WriteString("Node depth distribution:\n")
	for i, count := range s.depths {
		builder.WriteString(fmt.Sprintf("%d, %d\n", i, count))
	}

	return builder.String()
}

func GetNodeStatistics(trie *LiveTrie) (NodeStatistic, error) {
	collector := &nodeStatisticsCollector{}
	if err := trie.VisitAll(collector); err != nil {
		return NodeStatistic{}, err
	}
	return collector.stats, nil
}

type nodeStatisticsCollector struct {
	stats NodeStatistic
}

func (c *nodeStatisticsCollector) VisitAccount(_ NodeId, _ *AccountNode, depth int) {
	c.registerDepth(depth)
	c.stats.numAccounts++
}

func (c *nodeStatisticsCollector) VisitBranch(_ NodeId, b *BranchNode, depth int) {
	c.registerDepth(depth)
	c.stats.numBranches++
	numChildren := 0
	for _, child := range b.children {
		if !child.IsEmpty() {
			numChildren++
		}
	}
	c.stats.numChildren[numChildren]++
}

func (c *nodeStatisticsCollector) VisitExtension(_ NodeId, _ *ExtensionNode, depth int) {
	c.registerDepth(depth)
	c.stats.numExtensions++
}

func (c *nodeStatisticsCollector) VisitValue(_ NodeId, _ *ValueNode, depth int) {
	c.registerDepth(depth)
	c.stats.numValues++
}

func (c *nodeStatisticsCollector) registerDepth(depth int) {
	for len(c.stats.depths) <= depth {
		c.stats.depths = append(c.stats.depths, 0)
	}
	c.stats.depths[depth]++
}
