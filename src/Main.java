import mpi.MPI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    static List<Integer> mainThread(int maxN, int nrProcs, List<Integer> primesToSqrtN) {

        for (int i = 0; i < nrProcs; i++) {
            MpiUtils.sendObject(maxN, i + 1, 0);
            MpiUtils.sendObject(new ArrayList<>(primesToSqrtN), i + 1, 1);
        }

        List<Integer> collector = new ArrayList<>();
        for (int i = 0; i < nrProcs; i++) {
            ArrayList<Integer> values = MpiUtils.receiveObject(i + 1, 2);
            collector.addAll(values);
        }
        return collector.stream().sorted().collect(Collectors.toList());
    }

    static void worker(int rank, int nrProcs) {
        int maxN = MpiUtils.receiveObject(-1, 0);

        ArrayList<Integer> primesToSqrtN = MpiUtils.receiveObject(-1, 1);

        int begin = (maxN / nrProcs) * (rank - 1);
        int end = (maxN / nrProcs) * rank - 1;

        ArrayList<Integer> primesFound = new ArrayList<>();
        for (int i = Math.max(begin, 2); i < end; i++) {
            boolean primeFlag = true;
            for (Integer prime : primesToSqrtN) {
                if (i % prime == 0 && i != prime) {
                    primeFlag = false;
                    break;
                }
            }
            if (primeFlag) {
                primesFound.add(i);
            }
        }

        MpiUtils.sendObject(primesFound, 0, 2);
    }

    public static void main(String[] args) {

        MPI.Init(args);
        int nrProcs = Integer.parseInt(args[1]) - 1;
        int me = MPI.COMM_WORLD.Rank();
        int tasks = MPI.COMM_WORLD.Size();

        MPI.COMM_WORLD.Barrier();

        if (me == 0) {
            List<Integer> integers = mainThread(100, nrProcs, Arrays.asList(2, 3, 5, 7));
            System.out.println(integers);
        } else {
            worker(me, nrProcs);
        }

        MPI.COMM_WORLD.Barrier();
        MPI.Finalize();
    }

}
