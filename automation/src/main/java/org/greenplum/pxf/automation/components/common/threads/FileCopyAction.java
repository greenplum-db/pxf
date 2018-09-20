package org.greenplum.pxf.automation.components.common.threads;

import java.util.concurrent.Callable;

import org.greenplum.pxf.automation.components.common.ShellSystemObject;

/**
 * Worker thread which copying file from local path to remote node. Returns 1 is success 0
 * elsewhere.
 */
public class FileCopyAction implements Callable<Integer> {
	private ShellSystemObject from;
	private ShellSystemObject to;
	private String fromPath;
	private String toPath;

	public FileCopyAction(ShellSystemObject from, ShellSystemObject to, String fromPath, String toPath) {
		this.from = from;
		this.to = to;
		this.fromPath = fromPath;
		this.toPath = toPath;
	}

	@Override
	public Integer call() throws Exception {
		ShellSystemObject connection = null;
		try {
			// create new connection for each thread so it will happen in parallel
			connection = new ShellSystemObject(true);
			connection.setHost(from.getHost());
			connection.init();
			connection.copyToRemoteMachine(to.getUserName(), to.getPassword(), to.getHost(), fromPath, toPath);
			return 1;
		} catch (Exception e) {
			return 0;
		} finally {
			connection.close();
		}
	}
}
