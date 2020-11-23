#pragma once

#include <iostream>
#include <iomanip>
#include <fstream>
#include <queue>
#include <thread>
#include <atomic>
#include <sstream>
#include <stdexcept>
#include "tweetoscopeCollectorParams.hpp"
#include <iterator>
#include <cppkafka/cppkafka.h>
#include <boost/heap/binomial_heap.hpp>
#include <map>
#include <utility>
#include <string>
#include <vector>
#include <tuple>


namespace tweetoscope {

  class Cascade;
/*   struct Processor;
  struct Processors; */

  using timestamp = std::size_t;
  
  namespace source {
    using idf = std::size_t;
  }
  namespace cascade {
    using idf = std::size_t;
    using ref   = std::shared_ptr<Cascade>;
    using ref_w = std::weak_ptr<Cascade>;  
  }
  //Class for storing tweet Information
  struct tweet {
    std::string type = "";
    std::string msg  = "";
    timestamp time   = 0;
    double magnitude = 0;
    source::idf source = 0;
    std::string info = "";
    cascade::idf key;  //Storing the cascade key in the tweet makes it easier to assign a tweet to a given cascade  
  };

inline std::string get_string_val(std::istream& is) {
    char c;
    is >> c; // eats  "
    std::string value;
    std::getline(is, value, '"'); // eats tweet", but value has tweet
    return value;
}
  inline std::istream& operator>>(std::istream& is, tweet& t) {
    // A tweet is  : {"type" : "tweet"|"retweet",
    //                "msg": "...",
    //                "time": timestamp,
    //                "magnitude": 1085.0,
    //                "source": 0,
    //                "info": "blabla"}
    std::string buf;
    char c;
    is >> c; // eats '{'
    is >> c; // eats '"'
    while(c != '}') {
      std::string tag;
      std::getline(is, tag, '"'); // Eats until next ", that is eaten but not stored into tag.
      is >> c;  // eats ":"
      if     (tag == "type")    t.type = get_string_val(is);
      else if(tag == "msg")     t.msg  = get_string_val(is);
      else if(tag == "info")    t.info = get_string_val(is);
      else if(tag == "t")       is >> t.time;
      else if(tag == "m")       is >> t.magnitude;
      else if(tag == "source")  is >> t.source;

      is >> c; // eats either } or ,
      if(c == ',')
        is >> c; // eats '"'
    }
    return is;
  }  

  //This is the comparison functor for boost queues
  struct cascade_ref_comparator {
    bool operator()(cascade::ref op1, cascade::ref op2) const; 
  };

 
  //We define our Priority queue type
  using priority_queue = boost::heap::binomial_heap<cascade::ref,boost::heap::compare<cascade_ref_comparator>>;
  
  class Cascade {
    public:
    std::vector<std::pair<timestamp,double>> tweets;  // Storing only times and magnitudes
    priority_queue::handle_type location; //this is needed to update the priority queue
    std::string msg;          // msg of the tweet
    timestamp obs;            // the time of the newest retweet
    timestamp start_t;        // the starting time of the cascade
    cascade::idf cid;         // the id of the cascade
    source::idf source_id;    // the id of the source of the tweet

    Cascade(const tweet& t) : cid(t.key),source_id(t.source),msg(t.msg),obs(t.time),start_t(t.time){
      tweets.push_back({t.time,t.magnitude});
    };
    Cascade() = default;     //  A tweet can construct a cascade
    virtual ~Cascade() {};
    
    bool operator<(const Cascade& other) const;

    friend std::ostream& operator<<(std::ostream& os, const Cascade& c);
  }; 


  //Processor manages cascades for tweets comming from the same source
  struct Processor {

    priority_queue cascade_queue;
    //Map of list of cascades per obsertion window
    std::map<timestamp, std::queue<cascade::ref_w>> partial_cascade;
    //Map of the cascades associated to the processor
    std::map<cascade::idf, cascade::ref_w> locations;
    params::collector parameters;
    //queue for handling cascades termination

    Processor() = default;
    // A given processor is constructed from a tweet and parameters 
    Processor(const tweet& t, params::collector p);
    virtual ~Processor() {};
  };

  struct Processor_map {
    // a map containing all the processors (every processor is associated with a source)
    std::map<source::idf, Processor> processors;
    // parameters from the config file
    params::collector parameters;
    // Producer as a member of Processors, to avoid to create several producers each time sending a kafka message
    cppkafka::Configuration config;
    cppkafka::Producer  producer;
    Processor_map() = default;
    Processor_map(const std::string& config_filename) : parameters(config_filename),
    config({{"metadata.broker.list", parameters.kafka.brokers},
      {"log.connection.close", false }}),producer(config) {};

    virtual ~Processor_map() {};
    // This removes a processor
    void operator-=(const source::idf& s);
    void operator+=(const tweet& t); //This operator handles adding tweets to given cascades in the processors map, creating a new processor for new sources and sending terminated and partial cascades    
  };

  std::string msg_cascade_series(const Cascade& c, timestamp obs); //msg to send to topic cascade_series
  std::string msg_cascade_properties(const Cascade& c); //msg to send to topic cascade_properties
  void producer_kafka(cascade::ref c_ptr,Processor_map& pr, timestamp obs, bool Terminated); 

}

