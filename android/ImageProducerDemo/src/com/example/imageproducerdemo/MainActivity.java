/*
 * This application is a simple demo to show how kafka publisher
 * works. We take an image and send by calling the Producer class
 */
package com.example.imageproducerdemo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import android.hardware.Camera;
import android.hardware.Camera.PictureCallback;
import android.os.Bundle;
import android.os.Environment;
import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.FrameLayout;

public class MainActivity extends Activity {
	
	private Camera mCamera;
    private ImagePreview mPreview;
    private FrameLayout preview;
    public static String message_id = "COM.EXAMPLE.IMAGEPRODUCERDEMO_FILEPATH";
    public static final String Directory_name = "BTP1";
    
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_main);

		if (checkCameraHardware(this)) {
			// Create an instance of Camera
	        mCamera = getCameraInstance();
	        if (mCamera == null)
	        	System.out.println("its null");
	        // Create our Preview view and set it as the content of our activity.
	        mPreview = new ImagePreview(this, mCamera);
	        preview = (FrameLayout) findViewById(R.id.camera_preview);
	        preview.addView(mPreview);
	        
	        // Add a listener to the Capture button
	        Button captureButton = (Button) findViewById(R.id.image_capture);
	        captureButton.setOnClickListener(
	        	    new View.OnClickListener() {
	        	        @Override
	        	        public void onClick(View v) {
	        	            // get an image from the camera
	        	            mCamera.takePicture(null, null, mPicture);
	        	        }
	        	    }
	        	);
		} else {
			Log.e("camera", "camera not available in this device");
		}
	}

	/** Check if this device has a camera */
	private boolean checkCameraHardware(Context context) {
	    if (context.getPackageManager().hasSystemFeature(PackageManager.FEATURE_CAMERA)){
	        // this device has a camera
	        return true;
	    } else {
	        // no camera on this device
	        return false;
	    }
	}
	
	/** A safe way to get an instance of the Camera object. */
	public static Camera getCameraInstance(){
	    Camera c = null;
	    try {
	        c = Camera.open(); // attempt to get a Camera instance
	    }
	    catch (Exception e){
	        Log.e("camera", e.getMessage());
	    }
	    return c; // returns null if camera is unavailable
	}
	
	private PictureCallback mPicture = new PictureCallback() {
		
		// This function will be called immediately when
		// the picture is taken
	    @Override
	    public void onPictureTaken(byte[] data, Camera camera) {

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
	        
	        // The file is stored in the memory and the file name is send to the
	        // the SendImageActivity class
	        Intent intent = new Intent(getBaseContext(), SendImageActivity.class);
	    	intent.putExtra(message_id, pictureFile.getAbsolutePath());
	    	startActivity(intent);
	    }
	};
	
	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.main, menu);
		return true;
	}

	@Override
	protected void onResume() {
		// TODO Auto-generated method stub
		super.onResume();
	}
	
	@Override
    protected void onPause() {
        super.onPause();
        releaseCamera();              // release the camera immediately on pause event
    }

	// The camera should be released when the application pauses
	// so that other applications can use it
    private void releaseCamera(){
    	if (mPreview != null) {
    		preview.removeView(mPreview);
        	mPreview.getHolder().removeCallback(mPreview);
        	mPreview = null;
        }
    	
        if (mCamera != null){
        	mCamera.setPreviewCallback(null);
            mCamera.release();        // release the camera for other applications
            mCamera = null;
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
}
