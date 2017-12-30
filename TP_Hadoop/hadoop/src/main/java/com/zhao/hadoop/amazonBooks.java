package com.zhao.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.MapWritable;



public class amazonBooks {
	public static class TokenizerMapper
    		extends Mapper<Object, Text, Text, IntWritable>{
	
		private final static IntWritable one = new IntWritable(1);
	    //private Text word = new Text();
	    private HashMap<Text,NewMapWritable> map;
	 
	    /*
	     protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();

	        stopWords = new HashSet<String>();
	        for(String word : conf.get("stop.words").split(",")) {
	            stopWords.add(word);
	        }
	    }
	     */
	    
	    /**
	     * Initialization
	     */
	    @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            map = new HashMap();
        }
	   
	    public void mapper(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
	    		//seperate the words by ","
	    		StringTokenizer itr = new StringTokenizer(value.toString(),",");
			
	    		//create a list for every element of a set of view of map
		    while (itr.hasMoreTokens()) {
		    		map.put(new Text(itr.nextToken()),new NewMapWritable());
		    }
		    
		    //for every item in the map, we add the item which are different from the key item to the list of the key item
		    for(HashMap.Entry<Text,NewMapWritable> e : map.entrySet()) {
                this.addIfDiff(e.getKey());
		    }
		    
	    }
	    
	    /**
	     * This function permit to compare the param p to key item 
	     * if different, we add this item to the list
	     * @param p
	     */
	    private void addIfDiff(Text p){
	    		NewMapWritable m;
            for (HashMap.Entry<Text,NewMapWritable> e: map.entrySet()) {
                if (!e.getKey().equals(p)){
                    m = e.getValue();
                    m.put(new Text(p.toString()),one);
                }
            }
        }
	}
	
	/**
     * This function is to rewrite the function toString who has already exist
     * This is to make the thing returned as type of string, not an adress
     */
    public static class NewMapWritable extends MapWritable {
    		@Override
    		public String toString() {
    			String stri = null ;
    			Set<Writable> ind = this.keySet();
    			for(Writable i : ind ) {
    				IntWritable nb = (IntWritable) this.get(i);
    				stri = i.toString() + " : " + nb.toString(); 
    			}
    			return stri;
    		}
    }
    
    public static class IntSumReducer extends Reducer<Text, NewMapWritable, Text, Text> {

    		/**
    		 * initialize and clean up map/reduce tasks
    		 */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
        
       
        public void reduce(Text key, Iterable<NewMapWritable> values,Context context) 
        		throws IOException, InterruptedException {
            NewMapWritable res = new NewMapWritable();
            
            for (NewMapWritable v : values) {
                res = this.addAllInL(res,v);
            }
            
            List<NewMapWritable.Entry> l = new ArrayList<NewMapWritable.Entry>(res.entrySet());
            
            Collections.sort(l, new Comparator<NewMapWritable.Entry>(){
            		public int compare(Map.Entry e, Map.Entry e2) {
            			return ((IntWritable)e.getValue()).get()-((IntWritable)e2.getValue()).get();
                }
            });
            
            res = new NewMapWritable();
            for(NewMapWritable.Entry m : l){
            		Writable k = (Text)m.getKey();
            		Writable v = (IntWritable)m.getValue();
                res.put(k,v);
            }
        }
        
        public static NewMapWritable addAllInL(NewMapWritable res, NewMapWritable sum) {
            Object[] obj = sum.keySet().toArray();
            Text k;
            for (int i=0; i<obj.length; i++) {
                k = (Text)obj[i];
                //if this item has already exist, we do the "sum"
                //if not, we just put the intWritable value to this item
                if (res.containsKey(k)) {
                		Writable v = new IntWritable(((IntWritable)res.get(k)).get() + ((IntWritable)sum.get(k)).get());
                    res.put(k,v);
                }else {
                		Writable v = new IntWritable(((IntWritable)sum.get(k)).get()); 
                    res.put(k,v);
                }
            }
            return res;
        }

    }
    
	public static class combine extends Reducer<Text, NewMapWritable, Text, Text> {}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Amazon Recommandation ");
        job.setJarByClass(amazonBooks.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(combine.class);
        job.setMapOutputValueClass(NewMapWritable.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
