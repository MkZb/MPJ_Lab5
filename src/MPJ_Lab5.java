import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class MPJ_Lab5 {
    static int N = 400; //Matrix and array size.
    static int P = 4; //Threads count. Set it so N is multiple of P.
    static int divisionThreshold = N / P; //Size of parts recursive functions data should consist of.

    //Only read data
    static float[][] MD;
    static float[][] MT;
    static float[][] MZ;
    static float[] B;
    static float[] D;

    //Write data
    static float a = 0;
    static float[][] MA = new float[N][N];
    static float[] E = new float[N];

    public static void main(String[] args) {
        ForkJoinPool fjp = ForkJoinPool.commonPool();
        ReentrantLock access_E = new ReentrantLock();
        ReentrantLock access_MA = new ReentrantLock();

        long start = System.currentTimeMillis();

        System.out.println("Program started");
        Data data = new Data(N);
        data.loadData("test2.txt");
        MD = data.parseMatrix(N);
        MT = data.parseMatrix(N);
        MZ = data.parseMatrix(N);
        B = data.parseVector(N);
        D = data.parseVector(N);
        System.out.println("Data successfully parsed");

        FindMax task1 = new FindMax(divisionThreshold, MD, 0, N, N);
        a = task1.invoke();
        CalcResult task2 = new CalcResult(divisionThreshold, a, B, D, E, MD, MT, MZ, MA, 0, N, N, access_E, access_MA);
        task2.invoke();

        try {
            long finish = System.currentTimeMillis();
            long timeExecuted = finish - start;
            File resultMA = new File("resultMA.txt");
            File resultE = new File("resultE.txt");
            FileWriter writer1 = new FileWriter("resultMA.txt");
            FileWriter writer2 = new FileWriter("resultE.txt");
            for (int j = 0; j < N; j++) {
                for (int k = 0; k < N; k++) {
                    //System.out.print(MA[j][k] + " ");
                    writer1.write(MA[j][k] + "\n");
                }
                //System.out.println();
            }

            for (int j = 0; j < N; j++) {
                //System.out.print(E[j] + " ");
                writer2.write(E[j] + "\n");
            }
            //System.out.println();
            writer1.close();
            writer2.close();
            System.out.println("Data successfully saved on disk");
            System.out.println(timeExecuted + " milliseconds spent on calculations");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}

class FindMax extends RecursiveTask<Float> {
    final int thresholdSize, N;
    float[][] data;
    float result = 0;
    int start, end;

    FindMax(int threshold, float[][] data, int start, int end, int N) {
        this.thresholdSize = threshold;
        this.N = N;
        this.data = data;
        this.start = start;
        this.end = end;
    }

    protected Float compute() {
        if ((end - start) == thresholdSize) {
            for (int j = 0; j < N; j++) {
                for (int k = start; k < end; k++) {
                    if (data[j][k] > result) result = data[j][k];
                }
            }
            return result;
        } else {
            FindMax t1 = new FindMax(thresholdSize, data, start, (start + end) / 2, N);
            FindMax t2 = new FindMax(thresholdSize, data, (start + end) / 2, end, N);
            invokeAll(t1, t2);
            float r1 = t1.join();
            float r2 = t2.join();
            if (r1 > result) result = r1;
            if (r2 > result) result = r2;
        }

        return result;
    }
}

class CalcResult extends RecursiveAction {
    ReentrantLock access_E, access_MA;
    final int thresholdSize, N;
    float[][] MD;
    float[][] MT;
    float[][] MZ;
    float[][] MA;
    float[] B;
    float[] D;
    float[] E;
    float a;
    int start, end;

    CalcResult(int threshold, float a, float[] B, float[] D, float[] E, float[][] MD, float[][] MT,
               float[][] MZ, float[][] MA, int start, int end, int N, ReentrantLock access_E,
               ReentrantLock access_MA) {
        this.thresholdSize = threshold;
        this.a = a;
        this.B = B;
        this.D = D;
        this.E = E;
        this.MD = MD;
        this.MT = MT;
        this.MZ = MZ;
        this.MA = MA;
        this.start = start;
        this.end = end;
        this.N = N;
        this.access_E = access_E;
        this.access_MA = access_MA;
    }

    protected void compute() {
        if ((end - start) == thresholdSize) {
            float[][] MTZpart = new float[N][thresholdSize];
            float[][] MTDpart = new float[N][thresholdSize];
            //Calc B*MD+D*MT
            for (int j = start; j < end; j++) {
                float[] arrayToAdd = new float[2 * N];
                for (int k = 0; k < N; k++) {
                    arrayToAdd[k] += B[k] * MD[j][k];
                    arrayToAdd[k + N] += D[k] * MT[j][k];
                }
                Arrays.sort(arrayToAdd);
                float res = 0;
                for (int k = 0; k < 2 * N; k++) {
                    res += arrayToAdd[k];
                }

                access_E.lock();
                try {
                    E[j] = res;
                } finally {
                    access_E.unlock();
                }
            }

            //Calc max(MD)*(MT+MZ)
            for (int j = 0; j < N; j++) {
                for (int k = start; k < end; k++) {
                    MTZpart[j][k - start] = a * (MT[j][k] + MZ[j][k]);
                }
            }

            //Calc max(MD)*(MT+MZ)-MT*MD
            for (int j = 0; j < N; j++) {
                for (int k = start; k < end; k++) {
                    float[] arrayToAdd = new float[N];
                    for (int l = 0; l < N; l++) {
                        arrayToAdd[l] = MT[j][l] * MD[l][k];
                    }
                    Arrays.sort(arrayToAdd);
                    MTDpart[j][k - start] = 0;
                    for (int l = 0; l < N; l++) {
                        MTDpart[j][k - start] += arrayToAdd[l];
                    }
                    access_MA.lock();
                    try {
                        MA[j][k] = MTZpart[j][k - start] - MTDpart[j][k - start];
                    } finally {
                        access_MA.unlock();
                    }
                }
            }
        } else {
            invokeAll(new CalcResult(thresholdSize, a, B, D, E, MD, MT, MZ, MA, start, (start + end) / 2, N, access_E, access_MA),
                    new CalcResult(thresholdSize, a, B, D, E, MD, MT, MZ, MA, (start + end) / 2, end, N, access_E, access_MA));
        }
    }
}