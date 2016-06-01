package spark.test.SPARK15685;

import java.io.Serializable;
import java.security.Permission;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;

public class LocalModeErrorHandlingTest implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Blocks {@link System#exit(int)} for all callers.
	 * 
	 * @throws SecurityException if {@link System#exit(int)} is called
	 */
	private static final class NoExitSecurityManager extends SecurityManager {
		private SecurityException firstSecurityException = null;
		
		@Override
		public void checkExit(int status) {
			final SecurityException securityException = new SecurityException("Caught System.exit() call with status: " + status);
			if (firstSecurityException == null) {
				firstSecurityException = securityException;
			}
			// don't allow System.exit() from Spark - it will attempt at least twice
			throw securityException;
		}

		@Override
		public void checkPermission(Permission perm) {
			// other than exit, we don't care, NOOP
		}

		@Override
		public void checkPermission(Permission perm, Object context) {
			// other than exit, we don't care, NOOP
		}

		public SecurityException getFirstSecurityException() {
			return firstSecurityException;
		}
	}

	/**
	 * SPARK-15685: Fail test if Spark in local mode attempts to exit the JVM via System.exit() for certain types of "fatal" {@link Error}.
	 */
	@Test
	public void testNoSystemExitInLocalMode() {

		final SecurityManager originalSecurityManager = System.getSecurityManager();
		final NoExitSecurityManager securityManager = new NoExitSecurityManager();
		
		try {
			System.setSecurityManager(securityManager);

			// Local mode
			final SparkConf sparkConf = new SparkConf().setAppName("StackOverflowErrorCausesSystemExitLocalMode")
					.setMaster("local");

			try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {
				final String[] arr = new String[] { "One", "Two", "Three" };
				final List<String> inputList = Arrays.asList(arr);
				final JavaRDD<String> inputRDD = ctx.parallelize(inputList);

				inputRDD.foreach(new VoidFunction<String>() {
					private static final long serialVersionUID = 1L;

					public void call(String input) throws Exception {
						// test NoClassDefFoundError (extends LinkageError)
						// which is not exempted "non-fatal" by eotjer Scala NonFatal() or Spark isFatalError()
						throw new NoClassDefFoundError("fake NoClassDefFoundError");

						// or test unterminated recursion, test StackOverflowError (extends VirtualMachineError)
						//call(input); // StackOverflowError
					}
				});
			}

		} catch (SecurityException e) {
			// don't expect this, Spark seems to swallow it during it's System.exit() attempts
			throw new AssertionError("Caught SecurityException from System.exit() call attempt", e);
		} catch (Exception e) {
			// anything else e.g. SparkException wrapping NoClassDefFoundError - expected
		} finally {
			System.setSecurityManager(originalSecurityManager);
		}

		if (securityManager.getFirstSecurityException() != null) {
			throw new AssertionError("Test SecurityManager captured System.exit() attempt from Spark", securityManager.getFirstSecurityException());
		}
	}

}
