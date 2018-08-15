package UTS;

import java.util.ArrayList;
import java.util.LinkedList;
import org.pcj.NodesDescription;
import org.pcj.PCJ;
import org.pcj.PcjFuture;
import org.pcj.RegisterStorage;
import org.pcj.StartPoint;
import org.pcj.Storage;

@RegisterStorage(UTS.Shared.class)
public class UTS implements StartPoint {

  public static int seed;
  public static int b;
  public static int d;
  public static int seqDepth;
  public double den;
  long count;
  ArrayList<TreeNode> nodes;

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.out.println(
          "Usage: UTS <hostfile> <seed> <depth> <sequential_depth> <branching_factor>");
      System.exit(1);
    }

    seed = Integer.parseInt(args[1]);
    d = Integer.parseInt(args[2]);
    seqDepth = Integer.parseInt(args[3]);
    b = Integer.parseInt(args[4]);
    PCJ.deploy(UTS.class, new NodesDescription(args[0]));
  }

  @Override
  public void main() {
    final long before = System.nanoTime();
    if (PCJ.myId() == 0) {
      den = Math.log(b / (1.0 + b));
      PCJ.broadcast(den, Shared.den);
    }

    PCJ.monitor(Shared.nodes);
    PCJ.barrier();
    if (PCJ.myId() == 0) {
      LinkedList<TreeNode> firstNodes = new LinkedList<>();
      TreeNode.push(new SHA1Rand(seed, d), firstNodes, den);
      count += TreeNode.processSequential(firstNodes, den, seqDepth) + 1;

      // distribute nodes
      int nodesPerThread = firstNodes.size() / PCJ.threadCount();
      for (int i = 0; i < PCJ.threadCount(); ++i) {
        int size = nodesPerThread;
        if (i == PCJ.threadCount() - 1) { // last one gets remainderl
          size = firstNodes.size();
        }
        ArrayList<TreeNode> list = new ArrayList<TreeNode>();
        for (int j = 0; j < size; ++j) {
          list.add(firstNodes.remove(0));
        }
        if (i == 0) {
          nodes = list;
        } else {
          PCJ.asyncPut(list, i, Shared.nodes);
        }
      }
    }

    if (PCJ.myId() != 0) {
      PCJ.waitFor(Shared.nodes, 1);
    }
    count += TreeNode.processDFS(nodes, den);
    PCJ.barrier();

    if (PCJ.myId() == 0) {
      PcjFuture<Long>[] lists = new PcjFuture[PCJ.threadCount() - 1];
      for (int i = 1; i < PCJ.threadCount(); ++i) {
        lists[i - 1] = PCJ.asyncGet(i, Shared.count);
      }
      for (int i = 1; i < PCJ.threadCount(); ++i) {
        count += lists[i - 1].get();
      }
    }

    if (PCJ.myId() == 0) {
      System.out.println("Result=" + count);
      final long after = System.nanoTime();
      System.out.println("Times:" + ((after - before) / 1E9) + " sec");
    }
  }

  @Storage(UTS.class)
  enum Shared {
    nodes,
    count,
    den
  }
}
