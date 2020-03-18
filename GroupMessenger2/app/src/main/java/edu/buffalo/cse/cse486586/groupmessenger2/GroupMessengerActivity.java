package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private String myPort;
    private String deadNode = "00000";
    Map<String, String> ports = new HashMap<String, String>() {
        {
            put(REMOTE_PORT0, "0");
            put(REMOTE_PORT1, "1");
            put(REMOTE_PORT2, "2");
            put(REMOTE_PORT3, "3");
            put(REMOTE_PORT4, "4");
        }
    };
    private Uri mUri;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.scheme("content");
        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
        mUri = uriBuilder.build();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, e.getMessage());
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        final EditText editText = (EditText) findViewById(R.id.editText1);
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.d(TAG, "onClick entered");
                String msg = editText.getText().toString() + "\n\n";
                editText.setText("");
                tv.append("Message sent: " + msg);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private int seqNumber = 0;
        private int lastAgreedSeqNo = 0;
        private int lastProposedSeqNo = 0;
        PriorityQueue<String> buffer = new PriorityQueue<String>(25, new bufferComparator());
        ReentrantLock lock = new ReentrantLock();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.d(TAG, "ServerTask entered");
            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {
                    Socket server = serverSocket.accept();
                    server.setSoTimeout(1000);
                    Log.d(TAG, "server connected");
                    DataInputStream inputStream = new DataInputStream(server.getInputStream());
                    DataOutputStream outputStream = new DataOutputStream(server.getOutputStream());
                    String msgType = inputStream.readUTF();
                    Log.d(TAG, "S: msgType received: " + msgType);
                    if (msgType.equals("msg")) {
                        String port = inputStream.readUTF(); // msg received
                        Log.d(TAG, "S: port received: " + port);
                        String seq = inputStream.readUTF();
                        Log.d(TAG, "S: seq received: " + seq);
                        String msg = inputStream.readUTF();
                        Log.d(TAG, "S: msg received: " + msg);
                        String msgReceived = port + " " + seq + " " + msg;
                        if (msgReceived != null) {
                            Log.d(TAG, "S: message received: " + msgReceived);
                            addToBuffer(msgReceived);
                            Log.d(TAG, "S: message added to buffer: " + msgReceived);
                            String proposedSeq = getProposedSeq();
                            outputStream.writeUTF(proposedSeq); // proposed seq #
                            outputStream.flush();
                            Log.d(TAG, "S: sent proposed seq #: " + proposedSeq);
                        } else {
                            Log.d(TAG, "S: null message received");
                        }
                    } else if (msgType.equals("ASN")) {
                        String acceptedSeq = inputStream.readUTF(); // final seq # received
                        Log.d(TAG, "S: Received final seq #: " + acceptedSeq);
                        outputStream.writeUTF("Received final seq #");
                        outputStream.flush();
                        String[] temp = acceptedSeq.split(" ", 4);
                        String msgToRmv = "0 " + temp[1] + " " + temp[2] + " " + temp[3];
                        String msgToAdd = "1 " + temp[1] + " " + temp[0] + " " + temp[3];  //-- 1 means msg is now ready to be delivered
                        Log.d(TAG, "S: removing message from buffer: " + msgToRmv);
                        removeFromBufferAndUpdateAgreedSeq(msgToRmv, Float.parseFloat(temp[0]));
                        Log.d(TAG, "S: adding message to buffer: " + msgToAdd);
                        addToBuffer(msgToAdd);
                        Log.d(TAG, "S: After adding to buffer: " + buffer);
                        Log.d(TAG, "S: dead node: " + deadNode);
                        publishMessage();
                    } else if (msgType.equals("deadNode")) {
                        deadNode = inputStream.readUTF();
                        Log.d(TAG, "S: Received dead node #: " + deadNode);
                        outputStream.writeUTF("Received dead node #");
                        outputStream.flush();
                        publishMessage();
                    }
                } catch (Exception e) {
                    Log.e(TAG, "ServerTask Exception: " + e.toString());
                    continue;
                } finally {
                    if (lock.isLocked())
                        lock.unlock();
                }
//                return null;
            }

        }

        private void publishMessage() {
            while (!buffer.isEmpty()) {
                String[] str = buffer.peek().split(" ", 4);
                if (Integer.parseInt(str[0]) == 1) {
                    String msg1 = buffer.poll().split(" ", 4)[3];
                    publishProgress(msg1);
                    Log.d(TAG, "S: message published: " + msg1);
                } else {
                    Log.d(TAG, "S: dead node: " + deadNode);
                    if (str[1].compareTo(deadNode) == 0) {
                        String s = buffer.poll();
                        Log.d(TAG, "S: message deleted from dead node's buffer: " + deadNode + " " + s);
                    } else
                        break;
                }
            }
        }

        private void removeFromBufferAndUpdateAgreedSeq(String msgToRmv, float agreedSeqNo) {
            lock.lock();
            lastAgreedSeqNo = (int) Math.ceil(agreedSeqNo);
            buffer.remove(msgToRmv);
            lock.unlock();
        }

        private String getProposedSeq() {
            lock.lock();
            int lastSeqNo = lastAgreedSeqNo > lastProposedSeqNo ? lastAgreedSeqNo : lastProposedSeqNo;
            String val = ++lastSeqNo + "." + ports.get(myPort);
            lastProposedSeqNo = lastSeqNo;
            lock.unlock();
            return val;
        }

        private void addToBuffer(String msgReceived) {
            lock.lock();
            buffer.add(msgReceived);
            lock.unlock();
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + "\t\n\n");

            try {
                String keyToInsert = String.valueOf(seqNumber);
                ContentValues keyValueToInsert = new ContentValues();
                keyValueToInsert.put(KEY_FIELD, keyToInsert);
                keyValueToInsert.put(VALUE_FIELD, strReceived);

                getContentResolver().insert(mUri, keyValueToInsert);
                Log.d(TAG, "insert done for key: " + keyToInsert);
                seqNumber++;
            } catch (Exception e) {
                Log.e(TAG, "insert key-value failed");
            }
            return;
        }
    }

    class bufferComparator implements Comparator<String> {
        public int compare(String s1, String s2) {
            float a = Float.parseFloat(s1.split(" ", 4)[2]);
            float b = Float.parseFloat(s2.split(" ", 4)[2]);
            if (a < b)
                return -1;
            else if (a > b)
                return 1;
            return 0;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        int i = 0;
        int lastSeqNo = 0;
        float[] proposedSeqList = {0, 0, 0, 0, 0};
        List<String> sockets = new CopyOnWriteArrayList<String>();

        @Override
        protected Void doInBackground(String... msgs) {
            Log.d(TAG, "ClientTask entered");
            sockets.add(REMOTE_PORT0);
            sockets.add(REMOTE_PORT1);
            sockets.add(REMOTE_PORT2);
            sockets.add(REMOTE_PORT3);
            sockets.add(REMOTE_PORT4);
            String msg = msgs[0];
            String seq = ++lastSeqNo + "." + ports.get(myPort);
            String msgToSend = myPort + " " + seq + " " + msg;
            String remotePort = REMOTE_PORT0;

            for (; i < 5 ; i++) {
                try {
                    remotePort = sockets.get(i);
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort)), 1000);
                    Log.d(TAG, "C: " + remotePort + " client socket created");
                    socket.setSoTimeout(2000);
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    outputStream.writeUTF("msg");
                    outputStream.writeUTF("0 " + myPort);  // msg sent-- 0 means msg is not deliverable
                    outputStream.writeUTF(seq);
                    outputStream.writeUTF(msg);
                    outputStream.flush();
                    Log.d(TAG, "C: message sent: " + msgToSend);  //msg format: 0/1 port seq# msg
                    String proposedSeq = inputStream.readUTF();  //received proposal seq #
                    proposedSeqList[i] = Float.parseFloat(proposedSeq);
                    Log.d(TAG, "C: received proposal seq # from " + remotePort + " :" + proposedSeq);
                } catch (SocketTimeoutException e) {
                    i--;
                    Log.e(TAG, "ClientTask Timeout Exception: " + e.toString());
                    continue;
                }
                catch (Exception e) {
                    deadNode = remotePort;
                    sockets.set(i, null);
                    Log.e(TAG, "ClientTask Exception: " + deadNode + " x " + e.toString());
                    continue;
                }
            }
            try {
                float selectedSeq = Float.NEGATIVE_INFINITY;
                for (float x : proposedSeqList) {
                    if (x > selectedSeq)
                        selectedSeq = x;
                }

                String acceptedSeq = selectedSeq + " " + msgToSend;
                for (int j = 0; j < 5; j++) {
                    String port = sockets.get(j);
                    try {
                        if(port != null) {
                            Socket socket = new Socket();
                            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port)), 1000);
                            DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                            outputStream.writeUTF("ASN");
                            outputStream.writeUTF(acceptedSeq);
                            outputStream.flush();
                            Log.d(TAG, "C: sent accepted seq # to " + port + " :" + acceptedSeq);
                            String ack = inputStream.readUTF();
                            Log.d(TAG, "C: acknowledgement received from server: " + ack);
                        }
                    } catch (IOException e) {
                        deadNode = port;
                        Log.e(TAG, "ClientTask socket IOException " + e.toString());
                    }
                }
            } catch (Exception e) {
                Log.e(TAG, "ClientTask socket IOException" + e.toString());
            }

            if (!deadNode.equals("00000")) {
                for (int j = 0; j < 5; j++) {
                    String port = sockets.get(j);
                    try {
                        if(port != null) {
                            Socket socket = new Socket();
                            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port)), 1000);
                            DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                            outputStream.writeUTF("deadNode");
                            outputStream.writeUTF(deadNode);
                            outputStream.flush();
                            Log.d(TAG, "C: sent dead node # to " + port + " :" + deadNode);
                            String ack = inputStream.readUTF();
                            Log.d(TAG, "C: acknowledgement received from server: " + ack);
                        }
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException " + e.toString());
                    }
                }
            }
            return null;
        }
    }
}
