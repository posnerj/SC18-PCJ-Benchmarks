package Pi;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.pcj.NodesDescription;
import org.pcj.PCJ;
import org.pcj.PcjFuture;
import org.pcj.RegisterStorage;
import org.pcj.StartPoint;
import org.pcj.Storage;

@RegisterStorage(Pi.Shared.class)
public class Pi implements StartPoint {
  public static int N = 0;
  public static long tasksPerThread = 0;

  public long points = 0;
  public long c;
  public long tasksPerPlace;

  public static AtomicLong remainingTasks;
  public static final Object LOCK = new Object();

  @Storage(Pi.class)
  enum Shared {
    c,
    points,
    tasksPerPlace
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Usage: Pi <hostfile> <N> <tasksPerThread>");
      System.exit(1);
    }

    N = Integer.parseInt(args[1]);
    tasksPerThread = Integer.parseInt(args[2]);
    PCJ.deploy(Pi.class, new NodesDescription(args[0]));
  }

  @Override
  public void main() {
    final long before = System.nanoTime();
    points = 1L << N;
    tasksPerPlace = PCJ.threadCount() / PCJ.getNodeCount() * tasksPerThread;

    PCJ.barrier();

    long myTasksPerPlace = PCJ.get(0, Shared.tasksPerPlace);

    remainingTasks = new AtomicLong(myTasksPerPlace);

    points = PCJ.get(0, Shared.points);
    long nAll = points;
    final long pointsPerTask = nAll / (myTasksPerPlace * PCJ.getNodeCount());
    long tmpCount = 0;

    PCJ.barrier();

    while (remainingTasks.decrementAndGet() >= 0) {
      for (long i = 0; i < pointsPerTask; i++) {
        final double x = 2 * ThreadLocalRandom.current().nextDouble() - 1.0;
        final double y = 2 * ThreadLocalRandom.current().nextDouble() - 1.0;
        tmpCount += (x * x + y * y <= 1) ? 1 : 0;
      }
    }

    c = tmpCount;

    PCJ.barrier();

    if (PCJ.myId() == 0) {
      PcjFuture<Long> cL[] = new PcjFuture[PCJ.threadCount()];
      long c0 = c;
      for (int p = 1; p < PCJ.threadCount(); p++) {
        cL[p] = PCJ.asyncGet(p, Shared.c);
      }
      for (int p = 1; p < PCJ.threadCount(); p++) {
        c0 = c0 + cL[p].get();
      }
      System.out.println("Pi is roughly " + 4.0 * c0 / nAll);
      final long after = System.nanoTime();
      System.out.println("Times:" + ((after - before) / 1E9) + " sec");
    }
  }
}
