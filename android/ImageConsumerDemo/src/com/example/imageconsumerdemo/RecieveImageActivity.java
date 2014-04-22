package com.example.imageconsumerdemo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import android.app.Activity;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Bundle;
import android.os.Environment;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.ImageView;

public class RecieveImageActivity extends Activity {
	private String topic;
	public static final String Directory_name = "BTP1";		// The image might be stored in this directory
	private ImageView mImageView;
	ArrayList<Byte[]> byte_array_list = null;
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_recieve_image);
		
		Intent intent = getIntent();
		topic = intent.getStringExtra(ChooseTopicActivity.message_id); // Topic obtained from intent
		
		/*
		 * we are creating a consumer object. For information about the parameters given, go the
		 * the KafkaSimpleConsumer. The third parameter is the partition number which is 0 here.
		 * We are having only 1 partition here. For big applications involving huge data, we might
		 * required multiple paritions, in that case.. we have to fetch data from all paritions
		 * simultanoesuly and merge it.
		 */
		KafkaSimpleConsumer kafka_consumer =  new KafkaSimpleConsumer();
		try {
			byte_array_list = kafka_consumer.execute("20", topic, "0", 
					KafkaProperties.kafkaServerURL, KafkaProperties.kafkaServerPort+"", "0").get();
			
			Log.d("success","topic is "+topic+" "+byte_array_list.size());
			
		} catch (Exception e) {
			Log.e("failure",e.getMessage());
		} 
		
		// byte_array_list is a list of byte arrays. Each byte array is an image here
		Byte[] bray = byte_array_list.get(byte_array_list.size()-1);	// Taking the last image
		byte[] byte_array = new byte[bray.length];		// Converting Byte[] to byte[]
 		for (int i=0; i<byte_array.length; i++) {
			byte_array[i] = bray[i];
		}
		
 		/*
 		 * Previewing the image
 		 */
		mImageView = (ImageView)findViewById(R.id.image);
		
		/* Get the size of the ImageView */
		int targetW = mImageView.getWidth();
		int targetH = mImageView.getHeight();

		/* Get the size of the image */
		BitmapFactory.Options bmOptions = new BitmapFactory.Options();
		bmOptions.inJustDecodeBounds = true;
		BitmapFactory.decodeByteArray(byte_array, 0, byte_array.length, bmOptions);
		int photoW = bmOptions.outWidth;
		int photoH = bmOptions.outHeight;
		
		/* Figure out which way needs to be reduced less */
		int scaleFactor = 1;
		if ((targetW > 0) || (targetH > 0)) {
			scaleFactor = Math.min(photoW/targetW, photoH/targetH);	
		}

		/* Set bitmap options to scale the image decode target */
		bmOptions.inJustDecodeBounds = false;
		bmOptions.inSampleSize = scaleFactor;
		bmOptions.inPurgeable = true;

		/* Decode the JPEG file into a Bitmap */
		Bitmap bitmap = BitmapFactory.decodeByteArray(byte_array, 0, byte_array.length, bmOptions);
		
		/* Associate the Bitmap to the ImageView */
		mImageView.setImageBitmap(bitmap);
		mImageView.setVisibility(View.VISIBLE);
	}

	public void SavePicture (byte[] data) {
		File pictureFile = getOutputMediaFile();
		
        if (pictureFile == null){
        	Log.e("file_creation", "File not created properly");
            System.out.println("Error creating media file");
            return;
        }

        try {
            FileOutputStream fos = new FileOutputStream(pictureFile);
            fos.write(data);
            fos.close();
        } catch (FileNotFoundException e) {
            Log.d("debug", "File not found: " + e.getMessage());
        } catch (IOException e) {
            Log.d("debug", "Error accessing file: " + e.getMessage());
        } 
	}
	
	/** Create a File for saving an image or video */
	private static File getOutputMediaFile(){
	    // To be safe, you should check that the SDCard is mounted
	    // using Environment.getExternalStorageState() before doing this.

	    File mediaStorageDir = new File(Environment.getExternalStoragePublicDirectory(
	              Environment.DIRECTORY_PICTURES), Directory_name);
	    // This location works best if you want the created images to be shared
	    // between applications and persist after your app has been uninstalled.

	    // Create the storage directory if it does not exist
	    if (! mediaStorageDir.exists()){
	        if (! mediaStorageDir.mkdirs()){
	            Log.d("MyCameraApp", "failed to create directory");
	            return null;
	        }
	    }

	    // Create a media file name
	    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
	    File mediaFile = new File(mediaStorageDir.getPath() + File.separator +
	        "IMG_"+ timeStamp + ".jpg");
	   
	    return mediaFile;
	}
	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.recieve_image, menu);
		return true;
	}

}
