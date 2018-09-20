package org.greenplum.pxf.automation.components.common.threads;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;

/**
 * This worker thread is executing single command on given ShellSystemObject. If success return 1
 * else return 0.
 */
public class CommandExecutionAction implements Callable<Integer> {
	private ShellSystemObject connection;
	private String commandToExecute;

	public CommandExecutionAction(ShellSystemObject connection, String commandToExecute) {
		this.connection = connection;
		this.commandToExecute = commandToExecute;
	}

	@Override
	public Integer call() throws Exception {
		try {
			connection.runCommand(commandToExecute);
			return 1;
		} catch (IOException | ShellCommandErrorException e) {
			return 0;
		}
	}
}
