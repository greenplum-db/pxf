package org.greenplum.pxf.automation.components.common.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.common.threads.CommandExecutionAction;
import org.greenplum.pxf.automation.components.common.threads.FileCopyAction;

/**
 * Parallel actions over list of {@link ShellSystemObject} connections
 */
public class ParallelShellActions {

	/**
	 * Copy file to a given {@link ShellSystemObject} list.
	 * 
	 * @param fromConnection from which connection to copy
	 * @param targetConnections to which connections list to copy file
	 * @param fromPath local path to file on fromConnection
	 * @param toPath remote path on targetConnections list
	 * @throws Exception
	 */
	public static void copyFile(ShellSystemObject fromConnection, List<? extends ShellSystemObject> targetConnections, String fromPath, String toPath) throws Exception {
		// prepare callable list
		List<Callable<Integer>> callableList = new ArrayList<Callable<Integer>>();
		// create FileCopyAction threads and add to the callableList
		for (ShellSystemObject targetConnction : targetConnections) {
			Callable<Integer> callable = new FileCopyAction(fromConnection, targetConnction, fromPath, toPath);
			callableList.add(callable);
		}
		// execute all callableList
		executeCallableList(callableList);
	}

	/**
	 * Run parallel command over a given {@link ShellSystemObject} list.
	 * 
	 * @param connections list of {@link ShellSystemObject}
	 * @param command command to execute over connections list
	 * @throws Exception
	 */
	public static void runParallelCommand(List<? extends ShellSystemObject> connections, String command) throws Exception {
		// prepare callable list
		List<Callable<Integer>> callableList = new ArrayList<Callable<Integer>>();
		// create CommandExecutionAction threads and add to the callableList
		for (ShellSystemObject connection : connections) {
			Callable<Integer> callable = new CommandExecutionAction(connection, command);
			callableList.add(callable);
		}
		// execute all callables
		executeCallableList(callableList);
	}

	/**
	 * Execution engine for given list of {@link Callable} using {@link Integer} as result.
	 * 
	 * @param callableList list of {@link Callable} to execute
	 * @throws Exception will be thrown if not all threads finished successfully.
	 */
	private static void executeCallableList(List<Callable<Integer>> callableList) throws Exception {
		// use FixedThreadPool according available processors * 3
		final int threadPool = Runtime.getRuntime().availableProcessors() * 3;
		ExecutorService executer = Executors.newFixedThreadPool(threadPool);
		// invoke callabale list and get Future list with results
		List<Future<Integer>> list = executer.invokeAll(callableList);
		// shutdown executer for more callables
		executer.shutdown();
		// wait for all callables to finish or timeout to expire
		executer.awaitTermination(Node._10_MINUTES, TimeUnit.MILLISECONDS);
		// sum results from all callables and throw exception if not match to nodes list size
		int sum = 0;
		for (Future<Integer> submit : list) {
			sum += submit.get();
		}
		if (sum != callableList.size()) {
			throw new Exception("One or more callable failed during execution");
		}
	}
}
