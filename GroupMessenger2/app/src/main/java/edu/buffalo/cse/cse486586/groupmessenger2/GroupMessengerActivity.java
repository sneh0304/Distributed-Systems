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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
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
    private String deadNode = " ";
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
        PriorityQueue<String> buffer = new PriorityQueue<String>(15, new bufferComparator());
        ReentrantLock lock = new ReentrantLock();
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.d(TAG, "ServerTask entered");
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
//                    serverSocket.setSoTimeout(500);
                    Socket server = serverSocket.accept();
//                    server.setSoTimeout(500);
                    Log.d(TAG, "server connected");
                    PrintWriter outputStream = new PrintWriter(server.getOutputStream(), true);
                    BufferedReader inputStream = new BufferedReader(new InputStreamReader(server.getInputStream()));
                    String msgReceived = inputStream.readLine(); // msg received
                    if (msgReceived != null) {
                        Log.d(TAG, "S: message received: " + msgReceived);
                        String port = msgReceived.split(" ", 2)[0];
                        msgReceived = msgReceived.split(" ", 2)[1];
                        addToBuffer(msgReceived);
                        Log.d(TAG, "S: message added to buffer: " + msgReceived);
                        Log.d(TAG, "S: Remote port: " + port + " var my port: " + myPort);
                        //if (Integer.parseInt(port) != Integer.parseInt(myPort)) {
                            String proposedSeq = getProposedSeq();
                            outputStream.println(proposedSeq); // proposed seq #
                            outputStream.flush();
                        //}
                        String acceptedSeq;
                        while (true) {
                            acceptedSeq = inputStream.readLine(); // final seq # received
                            if (acceptedSeq.length() > 0)
                                break;
                        }
                        Log.d(TAG, "S: Received final seq #: " + acceptedSeq + " len: " + acceptedSeq.length());
                        outputStream.println("Received final seq #");
                        outputStream.flush();
                        String [] temp = acceptedSeq.split(" ", 3);
                        String msgToRmv = "0 " + temp[1] + " " + temp[2];
                        String msgToAdd = "1 " + temp[0] + " " + temp[2];  //-- 1 means msg is now ready to be delivered
                        Log.d(TAG, "S: removing message to buffer: " + msgToRmv);
                        removeFromBufferAndUpdateProposedSeq(msgToRmv, Float.parseFloat(temp[0]));
                        Log.d(TAG, "S: adding message to buffer: " + msgToAdd);
                        addToBuffer(msgToAdd);
                        while (!buffer.isEmpty()) {
                            String [] str = buffer.peek().split(" ", 3);
                            if (Integer.parseInt(str[0]) == 1) {
                                String msg = buffer.poll().split(" ", 3)[2];
                                publishProgress(msg);
                            }
                            else
                                break;
                        }
//                        Log.d(TAG, "message published: " + msgReceived);
//                        outputStream.println("message published, go ahead and close the socket");
//                        Log.d(TAG, "message received acknowledgement sent from server");
                    } else {
                        Log.d(TAG, "null message received");
                    }
//                    server.close();
//                    Log.d(TAG, "server socket closed");
//                    inputStream.close();
//                    Log.d(TAG, "server input stream closed");
//                    outputStream.close();
//                    Log.d(TAG, "server output stream closed");
                }
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "ServerTask socket Timed out");
            } catch (IOException e) {
                Log.e(TAG, e.getMessage());
                Log.e(TAG, "ServerTask socket IOException");
            }

            return null;
        }

        private void removeFromBufferAndUpdateProposedSeq(String msgToRmv, float proposedSeqNo) {
            lock.lock();
            lastProposedSeqNo = (int) Math.ceil(proposedSeqNo);
            buffer.remove(msgToRmv);
            lock.unlock();
        }

        private String getProposedSeq() {
            lock.lock();
            int lastSeqNo = lastAgreedSeqNo > lastProposedSeqNo ? lastAgreedSeqNo : lastProposedSeqNo;
            String val = ++lastSeqNo + "." + ports.get(myPort);
            lastAgreedSeqNo = lastSeqNo;
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
                Log.e(TAG, e.getMessage());
                Log.e(TAG, "insert key-value failed");
            }
            return;
        }
    }

    class bufferComparator implements Comparator<String> {
        public int compare(String s1, String s2) {
            float a = Float.parseFloat(s1.split(" ", 0)[1]);
            float b = Float.parseFloat(s2.split(" ", 0)[1]);
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
        float [] proposedSeqList = {0, 0, 0, 0, 0};
        Socket [] sockets = new Socket[]{new Socket(),new Socket(),new Socket(),new Socket(),new Socket()};
        @Override
        protected Void doInBackground(String... msgs) {
            Log.d(TAG, "ClientTask entered");
            String msg = msgs[0];
            String seq = ++lastSeqNo + "." + ports.get(myPort);
            String msgToSend = seq + " " + msg;
            String remotePort = REMOTE_PORT0;
                try {
                    for (; i < 5; i++) {
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
                        sockets[i].connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort)));//, 1500);
                        Log.d(TAG, "C: " + remotePort + " client socket created");
//                        sockets[i].setSoTimeout(1500);
                        BufferedReader inputStream = new BufferedReader(new InputStreamReader(sockets[i].getInputStream()));
                        PrintWriter outputStream = new PrintWriter(sockets[i].getOutputStream(), true);
                        outputStream.println(myPort + " 0 " + msgToSend);  // msg sent-- 0 means msg is not deliverable
                        outputStream.flush();
                        Log.d(TAG, "C: message sent: " + msgToSend);
                        //inputStream.readLine();
                        //if (Integer.parseInt(remotePort) != Integer.parseInt(myPort)) {
                        String proposedSeq = inputStream.readLine();  //received proposal seq #
                        Log.d(TAG, "C: received proposal seq # from " + remotePort + " :" + proposedSeq);
                        proposedSeqList[i] = Float.parseFloat(proposedSeq);
                        //} else {
                        //    proposedSeqList[i] = Float.parseFloat(seq);
                        //}

//                    String ack = inputStream.readLine();
//                    Log.d(TAG, "acknowledgement received from server: " + ack);
//
//                    socket.close();
//                    Log.d(TAG, remotePort + " client socket closed");
//                    outputStream.close();
//                    Log.d(TAG, "client output stream closed");
//                    inputStream.close();
//                    Log.d(TAG, "client input stream closed");
                    }

                    float selectedSeq = Float.NEGATIVE_INFINITY;
                    for (float x : proposedSeqList) {
                        if (x > selectedSeq)
                            selectedSeq = x;
                    }

                    int j = 0;
                    String acceptedSeq = selectedSeq + " " + msgToSend;
                    while (j < 5) {
                        BufferedReader inputStream = new BufferedReader(new InputStreamReader(sockets[j].getInputStream()));
                        PrintWriter outputStream = new PrintWriter(sockets[j].getOutputStream(), true);
                        outputStream.println(acceptedSeq);
                        outputStream.flush();
                        Log.d(TAG, "C: sent accepted seq # to " + j + " :" + acceptedSeq);
                        String ack = inputStream.readLine();
                        Log.d(TAG, "C: acknowledgement received from server: " + ack);
                        j++;
                    }

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (SocketTimeoutException e) {
                    Log.e(TAG, "ClientTask socket Timed out");
                } catch (IOException e) {
                    Log.e(TAG, e.getMessage());
                    Log.e(TAG, "ClientTask socket IOException");
                }

            return null;
        }
    }
}
