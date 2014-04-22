package com.example.imageconsumerdemo;


import android.os.Bundle;
import android.app.Activity;
import android.content.Intent;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;

public class ChooseTopicActivity extends Activity {
	public static String message_id = "COM.EXAMPLE.IMAGECONSUMERDEMO_TOPIC";
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_image_receive);
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.image_receive, menu);
		return true;
	}

	/** Called when the user clicks the Send button */
	public void sendMessage(View view) {
	    // Do something in response to button
		EditText editText = (EditText) findViewById(R.id.edit_message);
		String topic = editText.getText().toString();	// The topic is obtained from the user
		// and sent to the next activity via intent
		
		 Intent intent = new Intent(getBaseContext(), RecieveImageActivity.class);
	     intent.putExtra(message_id, topic);
	     startActivity(intent);
	}
}
