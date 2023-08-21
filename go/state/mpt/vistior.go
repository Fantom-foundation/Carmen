package mpt

import "fmt"

type NodeInfo struct {
	Id NodeId

	Path  []Nibble
	Depth int

	Source NodeSource
}

type NodeVisitor interface {
	VisitAccount(*AccountNode, NodeInfo)
	VisitBranch(*BranchNode, NodeInfo)
	VisitEmpty(EmptyNode, NodeInfo)
	VisitExtension(*ExtensionNode, NodeInfo)
	VisitValue(*ValueNode, NodeInfo)
}

func VisitPreorder(visitor NodeVisitor, node Node, info NodeInfo) error {
	node.accept(visitor, info)
	switch node := node.(type) {
	case *BranchNode:
		for i, child := range node.children {
			if child.IsEmpty() {
				continue
			}
			node, err := info.Source.getNode(child)
			if err != nil {
				return err
			}
			subInfo := info
			subInfo.Id = child
			subInfo.Depth++
			subInfo.Path = append(info.Path, Nibble(i))
			if err := VisitPreorder(visitor, node, subInfo); err != nil {
				return err
			}
		}
	case *ExtensionNode:
		next, err := info.Source.getNode(node.next)
		if err != nil {
			return err
		}
		subInfo := info
		subInfo.Id = node.next
		subInfo.Depth++
		subInfo.Path = append(info.Path, node.path.GetNibbles()...)
		return VisitPreorder(visitor, next, subInfo)
	}
	return nil
}

type HashPrinter struct{}

func (h HashPrinter) VisitAccount(node *AccountNode, info NodeInfo) {
	h.visit(node, info)
	hash, err := info.Source.getHashFor(node.storage)
	if err != nil {
		fmt.Printf("\tRoot: failed %v\n", err)
		return
	}
	fmt.Printf("\tRoot: %x\n", hash)
	fmt.Printf("Begin Storage\n")
	if err := info.Source.(*Forest).VisitAll(node.storage, HashPrinter{}); err != nil {
		fmt.Printf("Failed: %v\n", err)
	}
	fmt.Printf("End Storage\n")
}
func (h HashPrinter) VisitBranch(node *BranchNode, info NodeInfo)       { h.visit(node, info) }
func (h HashPrinter) VisitEmpty(node EmptyNode, info NodeInfo)          { h.visit(node, info) }
func (h HashPrinter) VisitExtension(node *ExtensionNode, info NodeInfo) { h.visit(node, info) }
func (h HashPrinter) VisitValue(node *ValueNode, info NodeInfo) {
	h.visit(node, info)
	fmt.Printf("\tPathLength: %d, Path: %v, Value: %v\n", node.pathLength, node.key, node.value)
}

func (HashPrinter) visit(node Node, info NodeInfo) {
	for _, n := range info.Path {
		fmt.Printf("%v", n)
	}
	fmt.Printf(" - ")
	hash, err := info.Source.getHashFor(info.Id)
	if err == nil {
		fmt.Printf("%x\n", hash)
	} else {
		fmt.Printf("failed: %v\n", err)
	}
}
