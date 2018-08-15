package WordCount;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.pcj.NodesDescription;
import org.pcj.PCJ;
import org.pcj.PcjFuture;
import org.pcj.RegisterStorage;
import org.pcj.StartPoint;
import org.pcj.Storage;

@RegisterStorage(WordCount.Shared.class)
public class WordCount implements StartPoint {

  public static String TEXTFILE;
  public static int REPETITIONS;
  public String textFile;
  public int repetitions;
  public HashMap<String, Integer> counts;

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.out.println("Usage: WordCount <hostfile> <textfile> <repetitions>");
      System.exit(1);
    }

    final String hostfile = args[0];
    TEXTFILE = args[1];
    REPETITIONS = Integer.parseInt(args[2]);
    PCJ.deploy(WordCount.class, new NodesDescription(hostfile));
  }

  public void main() throws Throwable {
    final long before = System.nanoTime();
    textFile = TEXTFILE;
    repetitions = REPETITIONS;
    PCJ.barrier();

    repetitions = PCJ.get(0, Shared.repetitions);
    textFile = PCJ.get(0, Shared.textFile);
    int repetitionsPerThread = repetitions / PCJ.threadCount();
    if (PCJ.myId() == PCJ.threadCount() - 1) { // last one gets remainder
      repetitionsPerThread += repetitions % PCJ.threadCount();
    }
    counts = new HashMap<String, Integer>();

    PCJ.barrier();
    for (int i = 0; i < repetitionsPerThread; ++i) {
      final List<String> lines = Files.readAllLines(Paths.get(textFile), StandardCharsets.UTF_8);
      for (String line : lines) {
        final String[] words = line.split(" ");
        for (String word : words) {
          int oldValue = counts.getOrDefault(word, 0);
          counts.put(word, oldValue + 1);
        }
      }
    }
    PCJ.barrier();

    if (PCJ.myId() == 0) {
      PcjFuture<HashMap<String, Integer>> maps[] = new PcjFuture[PCJ.threadCount() - 1];
      for (int i = 1; i < PCJ.threadCount(); ++i) {
        maps[i - 1] = PCJ.asyncGet(i, Shared.counts);
      }
      // reduce
      for (PcjFuture<HashMap<String, Integer>> future : maps) {
        HashMap<String, Integer> map = future.get();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
          int oldValue = counts.get(entry.getKey());
          counts.put(entry.getKey(), oldValue + entry.getValue());
        }
      }

      long sum = 0;
      for (Map.Entry<String, Integer> entry : counts.entrySet()) {
        System.out.println(entry.getKey() + ":" + entry.getValue());
        sum += entry.getValue();
      }
      System.out.println("sum=" + sum);
      final long after = System.nanoTime();
      System.out.println("Times:" + ((after - before) / 1E9) + " sec");
    }
  }

  @Storage(WordCount.class)
  enum Shared {
    counts,
    textFile,
    repetitions
  }
}
