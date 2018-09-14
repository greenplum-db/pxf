package org.greenplum.pxf.automation.components.common.cli;

import java.io.PrintStream;
import java.util.ArrayList;

import systemobject.terminal.BufferInputStream;
import systemobject.terminal.Cli;
import systemobject.terminal.Prompt;
import systemobject.terminal.RS232;
import systemobject.terminal.SSH;
import systemobject.terminal.Telnet;
import systemobject.terminal.VT100FilterInputStream;

import com.aqua.sysobj.conn.CliConnectionImpl;
import com.aqua.sysobj.conn.Position;

/**
 * extends {@link CliConnectionImpl} for PXF needs. Using the {@link PivotalSshRsa} modified SSH
 * class for using private key connection.
 */
public class PivotalCliConnectionImpl extends CliConnectionImpl {

	private ArrayList<Prompt> prompts = new ArrayList<Prompt>();;

	public PivotalCliConnectionImpl() {
		setDump(true);
		setUseTelnetInputStream(true);
		setProtocol("ssh-rsa");
		setPort(22);
	}

	public PivotalCliConnectionImpl(String host, String user, String password) {
		this();
		setUser(user);
		setPassword(password);
		setHost(host);
	}

	@Override
	public void init() throws Exception {
		super.init();
	}

	@Override
	public Position[] getPositions() {
		return null;
	}

	public Prompt[] getPrompts() {
		ArrayList<Prompt> prompts = new ArrayList<Prompt>();
		Prompt p = new Prompt();
		p.setCommandEnd(true);
		p.setPrompt("# ");
		prompts.add(p);

		p = new Prompt();
		p.setPrompt("login: ");
		p.setStringToSend(getUser());
		prompts.add(p);

		p = new Prompt();
		p.setPrompt("Password: ");
		p.setStringToSend(getPassword());
		prompts.add(p);
		return prompts.toArray(new Prompt[prompts.size()]);
	}

	@Override
	public void connect() throws Exception {

		activateIdleMonitor();

		connectRetries = connectRetries <= 0 ? 1 : connectRetries;

		for (int retriesCounter = 0; retriesCounter < connectRetries; retriesCounter++) {
			try {
				report.setFailToPass(true);
				internalConnect();

				break;
			} catch (Exception e) {
				report.report("Failed connecting  " + getHost() + ". Attempt " + (retriesCounter + 1) + ".  " + e.getMessage());
				try {
					disconnect();
				} catch (Exception t) {
				}
				if (retriesCounter == connectRetries - 1) {
					throw e;
				}
			} finally {
				report.setFailToPass(false);
			}
		}

		terminal.addFilter(new VT100FilterInputStream());
	}

	private void internalConnect() throws Exception {
		if (host == null) {
			throw new Exception("Default connection ip/comm is not configured");
		}
		report.report("Init cli, host: " + host);
		if (dummy) {
			return;
		}
		// Terminal t;
		boolean isRs232 = false;

		boolean isRsa = false;
		if (host.toLowerCase().startsWith(EnumConnectionType.COM.value()) || protocol.toLowerCase().equals(EnumConnectionType.RS232.value())) {
			// syntax for serial connection found
			isRs232 = true;
			String[] params = host.split("\\;");
			if (params.length < 5) {
				throw new Exception("Unable to extract parameters from host: " + host);
			}
			terminal = new RS232(params[0], Integer.parseInt(params[1]), Integer.parseInt(params[2]), Integer.parseInt(params[3]), Integer.parseInt(params[4]));
		} else if (protocol.toLowerCase().equals(EnumConnectionType.SSH.value())) {
			terminal = new SSH(host, user, password);
		} else if (protocol.toLowerCase().equals(EnumConnectionType.SSH_RSA.value())) {
			terminal = new PivotalSshRsa(host, user, password, getPrivateKey());
			isRsa = true;
		} else {
			terminal = new Telnet(host, port, useTelnetInputStream);
			if (dump) {
				((Telnet) terminal).setVtType(null);
			}
		}

		terminal.setCharSet(getCharSet());

		terminal.setIgnoreBackSpace(isIgnoreBackSpace());

		if (delayedTyping) {
			terminal.setKeyTypingDelay(keyTypingDelay);
		}
		cli = new Cli(terminal);

		// direct the "runtime" cli output to temp file
		cli.setPrintStream(new PrintStream("/dev/null"));
		if (enterStr != null) {
			setEnterStr(enterStr);
		}
		cli.setGraceful(graceful);
		if (useBuffer) {
			buffer = new BufferInputStream();
			terminal.addFilter(buffer);
			buffer.startThread();
		}

		if (vt100Filter) {
			terminal.addFilter(new VT100FilterInputStream());
		}
		Prompt[] prompts = getAllPrompts();
		for (int i = 0; i < prompts.length; i++) {
			cli.addPrompt(prompts[i]);
		}
		if (isRs232 || leadingEnter) {
			cli.command("");
		} else if (isRsa) {
			// Set remote connection to use bash with # as prompt
			cli.command("env PS1='#' bash --norc");
		} else {
			cli.login(60000, delayedTyping);
		}
		connected = true;
	}

	@Override
	public void addPrompts(Prompt[] promptsToAdd) {
		if (promptsToAdd == null) {
			return;
		}
		for (Prompt p : promptsToAdd) {
			if (terminal != null) {
				terminal.addPrompt(p);
			}
			prompts.add(p);
		}
	}

	private Prompt[] getAllPrompts() {
		ArrayList<Prompt> allPrompts = new ArrayList<Prompt>();
		allPrompts.addAll(prompts);
		Prompt[] pr = getPrompts();
		for (Prompt p : pr) {
			allPrompts.add(p);
		}
		return allPrompts.toArray(new Prompt[0]);
	}
}
