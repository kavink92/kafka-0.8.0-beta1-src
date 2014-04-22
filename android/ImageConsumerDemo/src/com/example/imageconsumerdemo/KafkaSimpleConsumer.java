package com.example.imageconsumerdemo;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
 
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dos.pubsub.document.BinaryDocument;
import com.dos.pubsub.document.Document;
import com.dos.pubsub.document.DocumentDecoder;
import com.dos.pubsub.document.StringDocument;

import android.os.AsyncTask;
import android.util.Log;
 
public class KafkaSimpleConsumer extends AsyncTask<String, Void, ArrayList<Byte[]>> {
	private Long byte_offset;
	private Long maxReads;
	private String topic;
	private int partition;
	private List<String> seeds = new ArrayList<String>();
	private int port;
	private List<String> m_replicaBrokers = new ArrayList<String>();
	ArrayList<Byte[]> bytes_list = new ArrayList<Byte[]>();
	 
	protected ArrayList<Byte[]> doInBackground(String... params) {

		// Maximum number of times the message is fetched from the server
		this.maxReads = Long.parseLong(params[0]);						
		this.topic = params[1];											// Topic is the second param
		// Partition of the topic from which we are going to fetch
		this.partition = Integer.parseInt(params[2]);					
		this.seeds.add(params[3]);										// Broker IP
		this.port = Integer.parseInt(params[4]);						// Port no
		// We should remember the last offset of message we fetched.
		// This is prevent us from fetched the old data always
		this.byte_offset = Long.parseLong(params[5]);					
		
		 try {
	           run(maxReads, topic, partition, seeds, port);
	        } catch (Exception e) {			
	            System.out.println("Oops:" + e);
	             e.printStackTrace();
	        }
		return bytes_list;
	}
	 
    public KafkaSimpleConsumer() {
        m_replicaBrokers = new ArrayList<String>();
    }
 
    
    public void run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws Exception {
        // find the meta data about the topic and partition we are interested in
        //
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
        if (metadata == null) {
        	Log.d("metadata", "Can't find metadata for Topic and Partition. Exiting" );
            return;
        }
        if (metadata.leader() == null) {
        	Log.d("metadata", "Can't find Leader for Topic and Partition. Exiting" );
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + a_topic + "_" + a_partition;
 
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 3000000, clientName);
        // The last offset the client used
        long readOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        //long readOffset = byte_offset;
        int numErrors = 0;
        // List of chunks the publisher sent
        ArrayList<BinaryDocument> docs = new ArrayList<BinaryDocument>();
        Byte[] byte_array;
        int byte_array_length = 0;
        
        // This decoder is in pubsub.jar
        DocumentDecoder mydecoder = new DocumentDecoder(null);
        
        while (a_maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, 100000, 3000000, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(a_topic, a_partition, readOffset, 3000000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);
 
            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                Log.e("fetch_error","Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                continue;
            }
            numErrors = 0;
            long numRead = 0;
            
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                byte_offset = readOffset;
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                  
                BinaryDocument bin_doc = (BinaryDocument)mydecoder.fromBytes(bytes);
                
                // if it is the first chunk, we start collecting the chunks
                if (bin_doc.IsFirst()) {
                	docs = new ArrayList<BinaryDocument>();
                	byte_array_length = 0;
                }
                
                byte_array_length += bin_doc.data.length;
                docs.add(bin_doc);
                
                // If it is the last chunk, then we collect it and add it to the list
                // which is returned to the activity calling the KafkaSimpleConsumer
                if (bin_doc.IsLast()) {
                	byte_array = new Byte[byte_array_length];
                	int i=0;
                	int j;
                	for (BinaryDocument doc : docs) {
                		for (j=0; j<doc.data.length; j++) {
                			byte_array[i] = doc.data[j];
                			i++;
                		}
                	}
                	bytes_list.add(byte_array);
                }
               
                numRead++;
                a_maxReads--;
            }
            return; 
           /* if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                	ie.printStackTrace();
                }
            }
            a_maxReads--;*/
        }
        /*
        System.out.println("its over"); 
        if (consumer != null) consumer.close();*/
    }
 
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
 
        if (response.hasError()) {
            Log.e("response_error","Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
 
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        Log.e("learder_error","Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
 
    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
 
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                Log.e("partition_error","Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
    
    protected Long GetOffset() {
    	return byte_offset;
    }
}