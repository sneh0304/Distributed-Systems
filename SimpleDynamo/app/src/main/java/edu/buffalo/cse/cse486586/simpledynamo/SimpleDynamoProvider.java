package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String KEY_FIELD = "key";
	static final String VALUE_FIELD = "value";
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String FIRST_JOINEE = REMOTE_PORT0;
	static final int SERVER_PORT = 10000;

	private String myPort;
	private String successor, predecessor = null;
	private Uri mUri;
	private LinkedList<String> memberList = new LinkedList<String>();

	ReentrantLock rLock = new ReentrantLock();
	ReentrantLock wLock = new ReentrantLock();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.v("delete", selection);

		try {
			if (selection.equals("@")) {
				for (String fileName : getContext().fileList()) {
					Log.v(TAG, "Deleting content values: " + fileName);
					getContext().deleteFile(fileName);
				}
			} else if (selection.equals("*")) {
				String msg = "delete_all";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
			} else {
				try {
					String hKey = genHash(selection);
					String hMyPort;
					String hPredecessor;
					String target = "";
					for (int i = 0; i < 5; i++) {
						int port = Integer.parseInt(memberList.get(i)) / 2;
						hMyPort = genHash(String.valueOf(port));
						int pred;
						if (i == 0)
							pred = Integer.parseInt(memberList.getLast()) / 2;
						else
							pred = Integer.parseInt(memberList.get(i - 1)) / 2;
						hPredecessor = genHash(String.valueOf(pred));
						if (isTargetNode(hKey, hMyPort, hPredecessor)) {
							target = String.valueOf(i);
							break;
						}
					}

					String msg = "delete";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, target, selection);
				} catch (NoSuchAlgorithmException e) {
					Log.e(TAG, "Hashing failed " + e.toString());
				}
			}
		} catch (Exception e) {
//            Log.e(TAG, e.getMessage());
			Log.e(TAG, "Content value delete failed " + e.toString());
			return 0;
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String filename = (String) values.get(KEY_FIELD);
		String string = (String) values.get(VALUE_FIELD);
		try {
			String hKey = genHash(filename);
			String hMyPort;
			String hPredecessor;
			String target = "";
			for (int i = 0; i < 5; i++) {
				int port = Integer.parseInt(memberList.get(i)) / 2;
				hMyPort = genHash(String.valueOf(port));
				int pred;
				if (i == 0)
					pred = Integer.parseInt(memberList.getLast()) / 2;
				else
					pred = Integer.parseInt(memberList.get(i - 1)) / 2;
				hPredecessor = genHash(String.valueOf(pred));
				if (isTargetNode(hKey, hMyPort, hPredecessor)) {
					target = String.valueOf(i);
					break;
				}
			}

			String msg = "insert";
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, target, filename, string);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Hashing failed " + e.toString());
		}
		return uri;
	}

	private void write(String key, String value) {
		try {
			PrintWriter outputStream;
			outputStream = new PrintWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
			Log.v(TAG, "Writing content values: " + key + ": " + value);
			outputStream.println(value);
			outputStream.close();
		} catch (Exception e) {
			Log.e(TAG, "Content value write failed " + key + ":" + value + " " + e.toString());
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.scheme("content");
		uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
		mUri = uriBuilder.build();

		memberList.add(REMOTE_PORT0);
		memberList.add(REMOTE_PORT1);
		memberList.add(REMOTE_PORT2);
		memberList.add(REMOTE_PORT3);
		memberList.add(REMOTE_PORT4);
		Collections.sort(memberList, new Comparator<String>() {
			@Override
			public int compare(String lhs, String rhs) {
				try {
					int a1 = Integer.parseInt(lhs) / 2;
					String b1 = genHash(String.valueOf(a1));
					int a2 = Integer.parseInt(rhs) / 2;
					String b2 = genHash(String.valueOf(a2));
					return b1.compareTo(b2);
				} catch (Exception e) {
					Log.e(TAG, "Hashing failed " + e.toString());
				}
				return 0;
			}
		});

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, e.getMessage());
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
//		if (!myPort.equals(FIRST_JOINEE)) {
//			String msg = "node_join";
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, "start", myPort);
//		}
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
		Log.v("query", selection);
		String[] columnNames = {KEY_FIELD, VALUE_FIELD};
		MatrixCursor cursor = new MatrixCursor(columnNames);

		BufferedReader inputStream;
		String value;
		try {
			if (selection.equals("@")) {
				for (String fileName : getContext().fileList()) {
					inputStream = new BufferedReader(new InputStreamReader(getContext().openFileInput(fileName)));
					Log.v(TAG, "Reading content values: " + fileName);
					value = inputStream.readLine();
					inputStream.close();
					String[] res = {fileName, value};
					cursor.addRow(res);
				}
			} else if (selection.equals("*")) {
				String msg = "query_all";
				String queryRes = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
				System.out.println("query all res at query: " + queryRes);
				for (String keyValuePair : queryRes.split("-->")) {
					String[] temp = keyValuePair.split(" ");
					String key = temp[0];
					value = temp[1];
					String[] res = {key, value};
					cursor.addRow(res);
					System.out.println(res[0] + " " + res[1]);
				}
			} else {
				try {
					String hKey = genHash(selection);
					String hMyPort;
					String hPredecessor;
					String target = "";
					for (int i = 0; i < 5; i++) {
						int port = Integer.parseInt(memberList.get(i)) / 2;
						hMyPort = genHash(String.valueOf(port));
						int pred;
						if (i == 0)
							pred = Integer.parseInt(memberList.getLast()) / 2;
						else
							pred = Integer.parseInt(memberList.get(i - 1)) / 2;
						hPredecessor = genHash(String.valueOf(pred));
						if (isTargetNode(hKey, hMyPort, hPredecessor)) {
							target = String.valueOf(i);
							break;
						}
					}

					String msg = "query";
					String queryRes = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, target, selection).get();
					value = queryRes.trim().split(" ", 2)[1];
					String[] res = {selection, value};
					cursor.addRow(res);
				} catch (NoSuchAlgorithmException e) {
					Log.e(TAG, "Hashing failed " + e.toString());
					return null;
				}
			}
		} catch (Exception e) {
//            Log.e(TAG, e.getMessage());
			Log.e(TAG, "Content value read failed " + e.toString());
			e.printStackTrace();
			return null;
		}
		return cursor;
	}

	private String read(String selection) {
		BufferedReader inputStream;
		String value;
		try {
			inputStream = new BufferedReader(new InputStreamReader(getContext().openFileInput(selection)));
			Log.v(TAG, "Reading content values: " + selection);
			value = inputStream.readLine();
			inputStream.close();
		} catch (Exception e) {
			Log.e(TAG, "Content value read failed " + e.toString());
			e.printStackTrace();
			return null;
		}
		return value;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private boolean isTargetNode(String key, String curr, String pred) {
		if (key.compareTo(pred) > 0 && key.compareTo(curr) <= 0)
			return true;
		else if (key.compareTo(pred) > 0 && key.compareTo(curr) > 0 && curr.compareTo(pred) < 0)
			return true;
		else if (key.compareTo(pred) < 0 && key.compareTo(curr) <= 0 && curr.compareTo(pred) < 0)
			return true;
		return false;
	}

	private void nodeJoin(String port) {
		if (predecessor == null) {
			successor = predecessor = port;
			String msg = "set_succ_pred";
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port, myPort);
		} else {
			String hPort = port;
			String hMyPort = myPort;
			String hPredecessor = predecessor;
			try {
				int port1 = Integer.parseInt(port) / 2;
				hPort = genHash(String.valueOf(port1));
				int port2 = Integer.parseInt(myPort) / 2;
				hMyPort = genHash(String.valueOf(port2));
				int pred = Integer.parseInt(predecessor) / 2;
				hPredecessor = genHash(String.valueOf(pred));
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, e.getMessage());
				Log.e(TAG, "Hashing failed");
			}
			if (isTargetNode(hPort, hMyPort, hPredecessor)) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "set_succ", port, myPort);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "set_pred", port, predecessor);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "set_succ", predecessor, port);
				predecessor = port;

			} else {
				String msg = "node_join";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, "forward", port);
			}
		}

	}

	private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Log.d(TAG, "ServerTask entered");
			ServerSocket serverSocket = sockets[0];

			while (true) {
				try {
					Socket server = serverSocket.accept();
					Log.d(TAG, "server connected");
					DataInputStream inputStream = new DataInputStream(server.getInputStream());
					DataOutputStream outputStream = new DataOutputStream(server.getOutputStream());
					String msgType = inputStream.readUTF();
					Log.d(TAG, "S: msgType received: " + msgType);
					if (msgType.equals("node_join")) {
						String port = inputStream.readUTF();
						Log.d(TAG, "S: port received to find successor and predecessor: " + port);
						nodeJoin(port);
					} else if (msgType.equals("set_succ_pred")) {
						String port = inputStream.readUTF();
						successor = predecessor = port;
						outputStream.writeUTF("Done");
						Log.d(TAG, "s: " + "ack sent for set_succ_pred");
					} else if (msgType.equals("set_succ")) {
						String port = inputStream.readUTF();
						successor = port;
						outputStream.writeUTF("Done");
						Log.d(TAG, "s: " + "ack sent for set_succ");
					} else if (msgType.equals("set_pred")) {
						String port = inputStream.readUTF();
						predecessor = port;
						outputStream.writeUTF("Done");
						Log.d(TAG, "s: " + "ack sent for set_pred");
					} else if (msgType.equals("insert")) {
						String key = inputStream.readUTF();
						String val = inputStream.readUTF();
						wLock.lock();
						write(key, val);
						wLock.unlock();
						outputStream.writeUTF("Done");
						Log.d(TAG, "s: " + "ack sent for insert");
					} else if (msgType.equals("query")) {
						String selection = inputStream.readUTF();
						rLock.lock();
						String res = read(selection);
						rLock.unlock();
						outputStream.writeUTF(selection);
						outputStream.writeUTF(res);
						outputStream.flush();
						outputStream.writeUTF("Done");
						Log.d(TAG, "s: " + "ack sent for query");
					} else if (msgType.equals("query_all")) {
						Cursor resultCursor = query(mUri, null, "@", null, null);
						int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
						int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
						List<String> res = new ArrayList<String>();
						while (resultCursor.moveToNext()) {
							String returnKey = resultCursor.getString(keyIndex).trim();
							String returnValue = resultCursor.getString(valueIndex).trim();
							System.out.println("S: check: " + returnKey + " " + returnValue);
							res.add(returnKey + " " + returnValue);
						}
						resultCursor.close();
						outputStream.writeUTF(TextUtils.join("-->", res));
						outputStream.flush();
						outputStream.writeUTF("Done");
						Log.d(TAG, "s: " + "ack sent for query_all");
					} else if (msgType.equals("delete")) {
						String selection = inputStream.readUTF();
						getContext().deleteFile(selection);
						outputStream.writeUTF("Done");
						Log.d(TAG, "s: " + "ack sent for delete");
					} else if (msgType.equals("delete_all")) {
						delete(mUri, "@", null);
						outputStream.writeUTF("Done");
						Log.d(TAG, "s: " + "ack sent for delete_all");
					}

				} catch (Exception e) {
					Log.e(TAG, "ServerTask Exception: " + e.toString());
					continue;
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, String> {
		@Override
		protected String doInBackground(String... msgs) {
			Log.d(TAG, "ClientTask entered");
			String queryRes = "";
			String msgType = msgs[0];

			try {
				if (msgType.equals("node_join")) {
					String remotePort = REMOTE_PORT0;
					if (msgs[1].equals("forward"))
						remotePort = successor;
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort)), 1000);
					Log.d(TAG, "C: " + remotePort + " client socket created for node join");
					DataInputStream inputStream = new DataInputStream(socket.getInputStream());
					DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
					outputStream.writeUTF(msgType);
					outputStream.writeUTF(msgs[2]);
					outputStream.flush();
				} else if (msgType.equals("set_succ_pred") || msgType.equals("set_succ") || msgType.equals("set_pred")) {
					String remotePort = msgs[1];
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
					Log.d(TAG, "C: " + remotePort + " client socket created for updating successor/predecessor");
					DataInputStream inputStream = new DataInputStream(socket.getInputStream());
					DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
					outputStream.writeUTF(msgType);
					outputStream.writeUTF(msgs[2]);
					outputStream.flush();
					String ack = inputStream.readUTF();
					Log.d(TAG, "C: " + "ack received for setting successor/predecessor " + ack);
				} else if (msgType.equals("insert")) {
					int targetIdx = Integer.parseInt(msgs[1]);
					String remotePort;
					for (int i = 0; i < 3; i++) {
						remotePort = memberList.get((targetIdx + i) % 5);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
						Log.d(TAG, "C: " + remotePort + " client socket created to insert values");
						DataInputStream inputStream = new DataInputStream(socket.getInputStream());
						DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
						outputStream.writeUTF(msgType);
						outputStream.writeUTF(msgs[2]);
						outputStream.writeUTF(msgs[3]);
						outputStream.flush();
						String ack = inputStream.readUTF();
						Log.d(TAG, "C: " + "ack received for insert " + ack);
					}
				} else if (msgType.equals("query")) {
					int targetIdx = Integer.parseInt(msgs[1]);
					String remotePort = memberList.get((targetIdx + 2) % 5);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
					Log.d(TAG, "C: " + remotePort + " client socket created to query a value");
					DataInputStream inputStream = new DataInputStream(socket.getInputStream());
					DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
					outputStream.writeUTF(msgType);
					outputStream.writeUTF(msgs[2]);
					outputStream.flush();
					String key = inputStream.readUTF();
					String val = inputStream.readUTF();
					queryRes = key + " " + val;
					String ack = inputStream.readUTF();
					Log.d(TAG, "C: " + "ack received for query/delete " + ack);
				} else if (msgType.equals("delete")) {
					int targetIdx = Integer.parseInt(msgs[1]);
					String remotePort;
					for (int i = 0; i < 3; i++) {
						remotePort = memberList.get((targetIdx + i) % 5);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
						Log.d(TAG, "C: " + remotePort + " client socket created to delete values");
						DataInputStream inputStream = new DataInputStream(socket.getInputStream());
						DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
						outputStream.writeUTF(msgType);
						outputStream.writeUTF(msgs[2]);
						outputStream.flush();
						String ack = inputStream.readUTF();
						Log.d(TAG, "C: " + "ack received for insert " + ack);
					}
				} else if (msgType.equals("query_all") || msgType.equals("delete_all")) {
					String remotePort = REMOTE_PORT0;
					List<String> queryResList = new ArrayList<String>();
					for (int i = 0; i < 5; i++) {
						try {
							switch (i) {
								case 0:
									remotePort = REMOTE_PORT0;
									break;
								case 1:
									remotePort = REMOTE_PORT1;
									break;
								case 2:
									remotePort = REMOTE_PORT2;
									break;
								case 3:
									remotePort = REMOTE_PORT3;
									break;
								case 4:
									remotePort = REMOTE_PORT4;
									break;
							}
							Socket socket = new Socket();
							socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort)), 1000);
							Log.d(TAG, "C: " + remotePort + " client socket created to query/delete *");
							DataInputStream inputStream = new DataInputStream(socket.getInputStream());
							DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
							outputStream.writeUTF(msgType);
							outputStream.flush();
							if (msgType.equals("query_all")) {
								String intermediateRes = inputStream.readUTF();
								if (!intermediateRes.equals(""))
									queryResList.add(intermediateRes);
							}
							String ack = inputStream.readUTF();
							Log.d(TAG, "C: " + "ack received for query all/delete all " + ack);
						} catch (Exception e) {
							Log.e(TAG, "ClientTask Exception during query all/delete all: " + e.toString());
						}

					}
					if (msgType.equals("query_all"))
						queryRes = TextUtils.join("-->", queryResList);
					System.out.println("query all res: " + queryRes);
				}
			} catch (Exception e) {
				Log.e(TAG, "ClientTask Exception: " + e.toString());
			}
			return queryRes.trim();
		}
	}
}
