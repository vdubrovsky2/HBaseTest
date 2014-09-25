import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HBaseTest {

	static HConnection connection = null;

	public static void main(String[] args) {
		try {
			String request = args[0];
			int threadsCount = new Integer(args[2]);
			int count = new Integer(args[1]);

			Configuration conf = HBaseConfiguration.create();
			conf.set("zookeeper.znode.parent", "/hbase-unsecure");
			conf.set("hbase.zookeeper.quorum", "dmp-master.local,dmp-master2.local");
			connection = HConnectionManager.createConnection(conf);

			//First call for zookeeper init
			connection.getTable("resource");

			StringBuilder sb1 = new StringBuilder();
			StringBuilder sb2 = new StringBuilder();
			sb1.append("Test1 threads count ").append(threadsCount).append(" ").append(count).append(" request for each thread");
			sb2.append("Test2 pool threads count ").append(threadsCount).append(" ").append(threadsCount * count).append(" requests total");

			long[] sums1 = new long[threadsCount];
			int[] counts1 = new int[threadsCount];
			long time1 = System.currentTimeMillis();
			test1(sums1, counts1, threadsCount, count, request);
			time1 = System.currentTimeMillis() - time1;
			long sum1 = calculateSum(sums1);
			int count1 = calculateSum(counts1);
			double avg1 = (new Long(sum1)).doubleValue() / count1;
			sb1.append(", test time ").append(time1);
			sb1.append(", time sum ").append(sum1);
			sb1.append(", count ").append(count1);
			sb1.append(", average ").append(avg1);

			long[] sums2 = new long[threadsCount * count];
			long time2 = System.currentTimeMillis();
			test2(threadsCount, sums2, threadsCount * count, request);
			time2 = System.currentTimeMillis() - time2;
			long sum2 = calculateSum(sums2);
			double avg2 = (new Long(sum2)).doubleValue() / (threadsCount * count);
			sb2.append(", test time ").append(time2);
			sb2.append(", time sum ").append(sum2);
			sb2.append(", count ").append(threadsCount * count);
			sb2.append(", average ").append(avg2);

			System.out.println(sb1.toString());
			System.out.println(sb2.toString());
			System.exit(0);
		} catch (Throwable ignored) {
			ignored.printStackTrace();
		}
	}

	public static void test1(final long[] sums, final int[] counts, int threadsCount, final int count, final String domain) {
		ArrayList<Thread> threads = new ArrayList<Thread>();
		int i = 0;
		while (i < threadsCount) {
			final int finalI = i;
			threads.add(new Thread(new Runnable() {
				@Override
				public void run() {
					test1Proc(finalI, sums, counts, count, domain);
				}
			}));
			i++;
		}
		i = 0;
		while (i < threadsCount) {
			threads.get(i).start();
			i++;
		}
		i = 0;
		while (i < threadsCount) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			i++;
		}
		int x = 0;
	}

	public static void test1Proc(int threadId, long[] sums, int[] counts, int count, String domain) {
		int i = 0;
		Long sum = (long) 0;
		Integer mCount = 0;
		while (i < count) {
			long measure = measure(domain);
			if (measure != 0L) {
				sum += measure;
				mCount++;
			}
			i++;
		}
		sums[threadId] = sum;
		counts[threadId] = mCount;
	}

	public static void test2(final int threadsCount, final long[] sums, int count, final String domain) {
		ExecutorService service = Executors.newFixedThreadPool(threadsCount);
		int i = 0;
		while (i < count) {
			final int finalI = i;
			service.submit(new Runnable() {
				@Override
				public void run() {
					long measure = measure(domain);
					if (measure != 0L) {
						sums[finalI] = measure;
					}
				}
			});
			i++;
		}
		service.shutdown();
		try {
			service.awaitTermination(10, TimeUnit.HOURS);
		} catch (InterruptedException e) {

		}
	}

	public static long measure(String domain) {
		try {
			long start = System.currentTimeMillis();
			HTableInterface table = connection.getTable("resource");
			String url = reverseUrl(new URL(domain));
			Get get = new Get(Bytes.toBytes(url));
			Get get1 = get.addFamily(Bytes.toBytes("taxonomy"));
			Result result = table.get(get1);
			result.getFamilyMap(Bytes.toBytes("taxonomy"));
			table.close();
			return System.currentTimeMillis() - start;
		} catch (Throwable ignore) {
			return 0L;
		}
	}

	private static long calculateSum(long[] ar) {
		long sum = 0L;
		int i = 0;
		while (i < ar.length) {
			long val = ar[i];
			sum += val;
			i++;
		}
		return sum;
	}

	private static int calculateSum(int[] ar) {
		int sum = 0;
		int i = 0;
		while (i < ar.length) {
			long val = ar[i];
			sum += val;
			i++;
		}
		return sum;
	}

	public static String reverseUrl(URL url) throws UnsupportedEncodingException {
		String host = url.getHost();
		String file = url.getFile();
		String ref = url.getRef();
		String protocol = url.getProtocol();
		int port = url.getPort();

		if ("/".equals(file) && ref != null) file = ref;

		StringBuilder buf = new StringBuilder();

    /* reverse host */
		reverseAppendSplits(host, buf);

    /* add protocol */
		buf.append(':');
		buf.append(protocol);

    /* add port if necessary */
		if (port != -1) {
			buf.append(':');
			buf.append(port);
		}

    /* add path */
		if (file.length() > 0 && '/' != file.charAt(0)) {
			buf.append('/');
		}
		file = URLEncoder.encode(file, "UTF-8");
		buf.append(file);

		return buf.toString();
	}

	public static void reverseAppendSplits(String string, StringBuilder buf) {
		String[] splits = StringUtils.split(string, '.');
		if (splits.length > 0) {
			for (int i = splits.length - 1; i > 0; i--) {
				buf.append(splits[i]);
				buf.append('.');
			}
			buf.append(splits[0]);
		} else {
			buf.append(string);
		}
	}

}


