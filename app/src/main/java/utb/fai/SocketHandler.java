package utb.fai;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class SocketHandler {
	private static final String NAME_PROMPT = "Enter your name (without spaces):";

	/** mySocket je socket, o který se bude tento SocketHandler starat */
	Socket mySocket;

	/** client ID je řetězec ve formátu <IP_adresa>:<port> */
	String clientID;

	/** Jméno zobrazované ostatním, null dokud si ho klient nenastaví. */
	String name;

	/**
	 * Místnosti, ve kterých se klient nachází, v pořadí připojení. Přistupuje se
	 * k nim jen přes synchronizované metody třídy ActiveHandlers.
	 */
	Set<String> rooms = new LinkedHashSet<String>();

	/**
	 * activeHandlers je reference na množinu všech právě běžících SocketHandlerů.
	 * Potřebujeme si ji udržovat, abychom mohli zprávu od tohoto klienta
	 * poslat všem ostatním!
	 */
	ActiveHandlers activeHandlers;

	/**
	 * messages je fronta příchozích zpráv, kterou musí mít každý klient svoji
	 * vlastní - pokud bude přetížená nebo nefunkční klientova síť,
	 * čekají zprávy na doručení právě ve frontě messages
	 */
	ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(20);

	/**
	 * startSignal je synchronizační závora, která zařizuje, aby oba tasky
	 * OutputHandler.run() a InputHandler.run() začaly ve stejný okamžik.
	 */
	CountDownLatch startSignal = new CountDownLatch(2);

	/** outputHandler.run() se bude starat o OutputStream mého socketu */
	OutputHandler outputHandler = new OutputHandler();
	/** inputHandler.run() se bude starat o InputStream mého socketu */
	InputHandler inputHandler = new InputHandler();
	/**
	 * protože v outputHandleru nedovedu detekovat uzavření socketu, pomůže mi
	 * inputFinished
	 */
	volatile boolean inputFinished = false;

	public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
		this.mySocket = mySocket;
		clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
		this.activeHandlers = activeHandlers;
	}

	/** send zařadí zprávu do fronty tohoto klienta. */
	void send(String message) {
		if (!messages.offer(message))
			System.err.printf("Client %s message queue is full, dropping the message!\n", clientID);
	}

	class OutputHandler implements Runnable {
		public void run() {
			OutputStreamWriter writer;
			try {
				System.err.println("DBG>Output handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Output handler running for " + clientID);
				writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
				writer.write("\nYou are connected from " + clientID + "\n");
				writer.flush();
				while (!inputFinished) {
					String m = messages.take();// blokující čtení - pokud není ve frontě zpráv nic, uspi se!
					if (inputFinished)
						break; // probudil nás InputHandler, klient už je pryč
					writer.write(m + "\r\n"); // pokud nějaké zprávy od ostatních máme,
					writer.flush(); // pošleme je našemu klientovi
					System.err.println("DBG>Message sent to " + clientID + ":" + m + "\n");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.err.println("DBG>Output handler for " + clientID + " has finished.");

		}
	}

	class InputHandler implements Runnable {
		public void run() {
			try {
				System.err.println("DBG>Input handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Input handler running for " + clientID);
				String request = "";
				/**
				 * v okamžiku, kdy nás Thread pool spustí, přidáme se do množiny
				 * všech aktivních handlerů, aby chodily zprávy od ostatních i nám
				 */
				activeHandlers.add(SocketHandler.this);
				send(NAME_PROMPT);
				BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
				while ((request = reader.readLine()) != null) { // přišla od mého klienta nějaká zpráva?
					System.out.println("From client " + clientID + ": " + request);
					processLine(request.strip());
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				// i při násilném odpojení musíme uvolnit jméno, vlákno OutputHandleru i socket
				activeHandlers.remove(SocketHandler.this);
				inputFinished = true;
				messages.offer("OutputHandler, wakeup and die!");
				try {
					mySocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			System.err.println("DBG>Input handler for " + clientID + " has finished.");
		}

		/** Zpracuje jeden řádek od klienta: příkaz, jméno, nebo zprávu do místností. */
		private void processLine(String line) {
			if (line.isEmpty())
				return;
			if (line.startsWith("#")) {
				processCommand(line);
			} else if (name == null) {
				changeName(line); // první řádek po připojení je jméno, bez příkazu #setMyName
			} else if (!activeHandlers.sendMessageToRooms(SocketHandler.this, formatMessage(line))) {
				send("You are not in any room, your message was not delivered.");
			}
		}

		private void processCommand(String line) {
			String[] parts = line.split("\\s+", 2);
			String command = parts[0];
			String argument = parts.length > 1 ? parts[1] : "";

			if (command.equals("#setMyName")) {
				changeName(argument);
			} else if (name == null) {
				send("Set your name first. " + NAME_PROMPT);
			} else if (command.equals("#sendPrivate")) {
				sendPrivate(argument);
			} else if (command.equals("#join")) {
				join(argument);
			} else if (command.equals("#leave")) {
				leave(argument);
			} else if (command.equals("#groups")) {
				send(activeHandlers.getRooms(SocketHandler.this));
			} else {
				// překlep v příkazu nesmí skončit jako veřejná zpráva
				send("Unknown command: " + command);
			}
		}

		private void changeName(String newName) {
			if (!newName.matches("[^\\s#]\\S*")) {
				send("Invalid name: it must not be empty, contain spaces or start with #.");
			} else if (!activeHandlers.setName(SocketHandler.this, newName)) {
				send("Name " + newName + " is already taken.");
			} else {
				send("Your name is now " + newName);
				return;
			}
			if (name == null)
				send(NAME_PROMPT);
		}

		private void sendPrivate(String argument) {
			String[] parts = argument.split("\\s+", 2);
			if (parts.length < 2) {
				send("Usage: #sendPrivate <name> <message>");
			} else if (!activeHandlers.sendPrivateMessage(parts[0], formatMessage(parts[1]))) {
				send("User " + parts[0] + " is not connected.");
			}
		}

		private void join(String room) {
			if (isValidRoomName(room)) {
				activeHandlers.joinRoom(SocketHandler.this, room);
				send("You are in room " + room);
			}
		}

		private void leave(String room) {
			if (isValidRoomName(room)) {
				if (activeHandlers.leaveRoom(SocketHandler.this, room))
					send("You left room " + room);
				else
					send("You are not in room " + room);
			}
		}

		/** Čárka by rozbila výpis příkazu #groups, mezera formát příkazů. */
		private boolean isValidRoomName(String room) {
			if (room.matches("[^\\s,]+"))
				return true;
			send("Invalid room name: it must not be empty or contain spaces or commas.");
			return false;
		}

		private String formatMessage(String text) {
			return "[" + name + "] >> " + text;
		}
	}
}
