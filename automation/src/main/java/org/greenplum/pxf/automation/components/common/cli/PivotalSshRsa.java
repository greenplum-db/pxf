package org.greenplum.pxf.automation.components.common.cli;

import java.io.File;
import java.io.IOException;

import systemobject.terminal.SSH;
import ch.ethz.ssh2.Connection;

/**
 * Extends {@link SSH} to connect using private key if exists instead of password connection.
 *
 */
public class PivotalSshRsa extends SSH {

	private File privateKeyFile;

	public PivotalSshRsa(String hostnameP, String usernameP, String passwordP, File privateKey) {
		super(hostnameP, usernameP, passwordP);
		privateKeyFile = privateKey;

	}

	@Override
	public void connect() throws IOException {
		boolean isAuthenticated = false;
		/* Create a connection instance */
		System.out.println("Connet to Host with SSH and RSA private key");
		conn = new Connection(hostname);

		/* Now connect */
		conn.connect();

		// Check what connection options are available to us
		String[] authMethods = conn.getRemainingAuthMethods(username);
		System.out.println("The supported auth Methods are:");

		for (String method : authMethods) {
			System.out.println(method);
		}

		/* Authenticate */
		if (password != null && !password.equals("")) {
			super.connect();
		} else {
			// user not supplied password
			try {
				if (privateKeyFile != null && privateKeyFile.isFile()) {
					System.out.println("Connecting using Private Key");

					// connect using private key
					isAuthenticated = conn.authenticateWithPublicKey(username, privateKeyFile, "");
				} else {
					System.out.println("Auth Error - The privateKeyFile should be init from the SUT with a valid path to ppk/pem RSA private key");
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
				isAuthenticated = false;
			}
		}

		if (sourcePort > -1 && destinationPort > -1) {
			lpf = conn.createLocalPortForwarder(sourcePort, "localhost", destinationPort);
		}

		/* Create a session */
		sess = conn.openSession();

		/*
		 There are commands in automation that are terminal-aware and return colored output. This was
		 causing extraneous output when using xterm terminal in automation tests on RHEL9, causing tests to fail.
		 This does not happen when using a "dumb" PTY as the commands will not emit ANSI escape sequences.
		 */
		sess.requestDumbPTY();

		sess.startShell();

		in = sess.getStdout();
		out = sess.getStdin();
	}

	@Override
	public void disconnect() {
		super.disconnect();
	}

	@Override
	public boolean isConnected() {
		return super.isConnected();
	}

	@Override
	public String getConnectionName() {
		return "SSH_RSA";
	}

	public File getPrivateKeyFile() {
		return privateKeyFile;
	}

	public void setPrivateKeyFile(File privateKeyFile) {
		this.privateKeyFile = privateKeyFile;
	}
}
