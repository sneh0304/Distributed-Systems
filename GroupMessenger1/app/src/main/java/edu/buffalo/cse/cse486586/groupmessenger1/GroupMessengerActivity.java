package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

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

        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.scheme("content");
        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger1.provider");
        mUri = uriBuilder.build();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
//            serverSocket.setReuseAddress(true);
//            serverSocket.bind(new InetSocketAddress(SERVER_PORT));
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
        Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new View.OnClickListener() {
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
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.d(TAG, "ServerTask entered");
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                while (true) {
                    Socket server = serverSocket.accept();
                    Log.d(TAG, "server connected");
                    PrintWriter outputStream = new PrintWriter(server.getOutputStream(), true);
                    BufferedReader inputStream = new BufferedReader(new InputStreamReader(server.getInputStream()));
                    String msgReceived = inputStream.readLine();
                    if (msgReceived != null) {
                        Log.d(TAG, "message received: " + msgReceived);
                        publishProgress(msgReceived);
                        Log.d(TAG, "message published: " + msgReceived);
                        outputStream.println("message published, go ahead and close the socket");
                        Log.d(TAG, "message received acknowledgement sent from server");
                    } else {
                        Log.d(TAG, "null message received");
                    }
                    server.close();
                    Log.d(TAG, "server socket closed");
                    inputStream.close();
                    Log.d(TAG, "server input stream closed");
                    outputStream.close();
                    Log.d(TAG, "server output stream closed");
                }
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "ServerTask socket Timed out");
            } catch (IOException e) {
                Log.e(TAG, e.getMessage());
                Log.e(TAG, "ServerTask socket IOException");
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + "\t\n\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */

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

//            String filename = "GroupMessengerActivity";
//            String string = strReceived + "\n";
//            FileOutputStream outputStream;
//
//            try {
//                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
//                outputStream.write(string.getBytes());
//                outputStream.close();
//            } catch (Exception e) {
//                Log.e(TAG, "File write failed");
//            }

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Log.d(TAG, "ClientTask entered");
            try {
                String msgToSend = msgs[0];
                String remotePort = REMOTE_PORT0;
                for (int i = 0; i < 5; i++) {
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
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    Log.d(TAG, remotePort + " client socket created");
                    BufferedReader inputStream = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter outputStream = new PrintWriter(socket.getOutputStream(), true);
                    outputStream.println(msgToSend);
                    Log.d(TAG, "message sent: " + msgToSend);
                    String ack = inputStream.readLine();
                    Log.d(TAG, "acknowledgement received from server: " + ack);

                    socket.close();
                    Log.d(TAG, remotePort + " client socket closed");
                    outputStream.close();
                    Log.d(TAG, "client output stream closed");
                    inputStream.close();
                    Log.d(TAG, "client input stream closed");
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, e.getMessage());
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }
}
