/* 
 *  The class has the code for kafka publisher and
 *  its run in the background of the application since it has
 *  heavy weight network calls
 */
package com.example.imageproducerdemo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.dos.pubsub.document.BinaryDocument;


import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import android.os.AsyncTask;
import android.util.Log;

public class Producer extends AsyncTask<String, Void, Boolean>{

	private kafka.javaapi.producer.Producer<String, BinaryDocument> producer;
	private String topic;
	private Properties props = new Properties();
	private int MESSAGE_SIZE = 100000;		// We are sending message of size 100000bytes each time
	private boolean success = false;
	 
	public Producer() {
		// It contains the IP address of the broker and the port number of the form 10.9.6.52:9092
		String checking_IP = KafkaProperties.kafkaServerURL+":"+KafkaProperties.kafkaServerPort;
		// We use the serializer class in pubsub.jar file, default serializer does not do anything
	    props.put("serializer.class", "com.dos.pubsub.document.DocumentEncoder");
	    // The IP address of all the brokers if more than one are there
	    props.put("metadata.broker.list", checking_IP );
	    // We are using the default partitioner (ie) that is the message goes to a random partition
	    // We are currently having only one partition here.
	    producer = new kafka.javaapi.producer.Producer<String, BinaryDocument>(new ProducerConfig(props));
	}
	
	@Override
	protected Boolean doInBackground(String... params) {
		topic = params[0];
		File file = new File(params[1]);		// We open the file with the file_path obtained
	
	    byte[] buffer = new byte[(int) file.length()];
	    InputStream ios = null;
	    try {
	        ios = new FileInputStream(file);
	        if ( ios.read(buffer) == -1 ) {
	            throw new IOException("EOF reached while trying to read the whole file");
	        }        
	    } catch (Exception e) {
			e.printStackTrace();
			return success;
		} 
	    finally { 
	        try {
	             if ( ios != null ) 
	                  ios.close();
	        } catch ( IOException e) {
	        	e.printStackTrace();
	        	return success;
	        }
	    }
		
		byte[] bytes_to_be_sent ;
		int start=0;
		int end,counter;
		
		// Small hack to split the bytes into chunks.
		while (start<buffer.length ) {
			end = Math.min(buffer.length, start+MESSAGE_SIZE);
			bytes_to_be_sent = new byte[end-start];
			counter = 0;
			for (; start<end; start++) {
				bytes_to_be_sent[counter] = buffer[start];
				counter++;
			}
			
			BinaryDocument bdoc = new BinaryDocument(bytes_to_be_sent);
			// The is the first chunk of message sent.
			if (start==0) {
				bdoc.SetFirst(true);
			}
			
			// This is the last of the chunk send
			if (end >= buffer.length)
				bdoc.SetLast(true);
			
			// we are sending the topic as key and the Binary_document as the message
			producer.send(new KeyedMessage<String, BinaryDocument>(topic, bdoc));
			Log.d("success","The byte message length is "+bytes_to_be_sent.length);
		}
		
		file.delete();		// We dont need the file after sending it to the broker
		success = true;
		return success;
	}

}
