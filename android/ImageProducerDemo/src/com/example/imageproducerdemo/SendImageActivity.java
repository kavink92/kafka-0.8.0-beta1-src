package com.example.imageproducerdemo;

import java.util.concurrent.ExecutionException;

import android.app.Activity;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

public class SendImageActivity extends Activity {

	private String topic;					// It is obtained from the user
	private String file_path;				// It is received through the intent
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_send_image);
		
		Intent intent = getIntent();
		// File path is received from prev activity
		file_path = intent.getStringExtra(MainActivity.message_id);		

	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.send_image, menu);
		return true;
	}
	
	/** Called when the user clicks the Send button */
	public void sendMessage(View view) {
	    // Do something in response to button
		EditText editText = (EditText) findViewById(R.id.edit_message);
		topic = editText.getText().toString(); // topic is obtained
		TextView textView = (TextView)findViewById(R.id.text);
		try {
			
			/* 
			 *  The execute function in producer code is called. The Producer class implements
			 *  the  AsyncTask class and this run in a background thread and won't interrupt 
			 *  the activity. 
			 */
			boolean success = (new Producer().execute(topic, file_path)).get();
			
			if (success) 
				textView.setText("The message has been sent successfully");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
