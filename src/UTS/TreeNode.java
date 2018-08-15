package UTS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class TreeNode implements Serializable {
  private static final long serialVersionUID = 1L;

  protected SHA1Rand parent;
  protected int id;

  public static long push(SHA1Rand sha1Rand, List<TreeNode> queue, double den) {
    final int u = (int) Math.floor(Math.log(1 - sha1Rand.getRand() / 2147483648.0) / den);

    if (sha1Rand.getDepth() > 1) {
      for (int i = 0; i < u; ++i) {
        queue.add(new TreeNode(sha1Rand, i));
      }
      return 0;
    }
    return u;
  }

  public long expand(List<TreeNode> queue, double den) {
    int depth = this.getParent().getDepth();
    if (depth > 1) {
      SHA1Rand sha1Rand = new SHA1Rand(this.getParent(), this.getId(), depth - 1);
      return push(sha1Rand, queue, den);
    }
    return 0;
  }

  public static long processDFS(List<TreeNode> nodes, double den) {
    long count = 0;
    while (!nodes.isEmpty()) {
      ++count;
      TreeNode currentNode = nodes.remove(nodes.size() - 1);
      count += currentNode.expand(nodes, den);
    }
    return count;
  }

  /*
    Counts the nodes using BFS for the first maxDepth levels.
    Modifies nodes to only contain the remainig nodes.
    Returns the count.

    All nodes in nodes-list must be on the same level.
  */
  public static long processSequential(LinkedList<TreeNode> nodes, double den, int levels) {
    final Deque<TreeNode> queue = nodes;
    long count = 0;

    TreeNode currentNode = queue.peekFirst();
    final int targetDepth = currentNode.getParent().getDepth() - levels;

    ArrayList<TreeNode> newNodes = new ArrayList<TreeNode>();
    while (!queue.isEmpty() && currentNode.getParent().getDepth() > targetDepth) {
      currentNode = queue.pollFirst();
      count += currentNode.expand(newNodes, den) + 1;
      for (TreeNode node : newNodes) {
        queue.addLast(node);
      }
      newNodes.clear();
    }

    return count;
  }

  public TreeNode(SHA1Rand parent, int id) {
    this.id = id;
    this.parent = parent;
  }

  public SHA1Rand getParent() {
    return parent;
  }

  public void setParent(SHA1Rand parent) {
    this.parent = parent;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }
}
