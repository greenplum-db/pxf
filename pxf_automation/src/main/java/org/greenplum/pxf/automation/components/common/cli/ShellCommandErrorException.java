package org.greenplum.pxf.automation.components.common.cli;

/**
 * This exception will be thrown whenever shell command will fail. If the fail is expected please
 * catch this exception.
 */
@SuppressWarnings("serial")
public class ShellCommandErrorException extends Exception {

	public ShellCommandErrorException(String message) {
		super(message);
	}
}
