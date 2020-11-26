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

  struct Cascade;
  struct Processor;
  struct Processors;

  using timestamp = std::size_t;
  
  namespace source {
    using idf = std::size_t;
  }
  namespace cascade {
    using ref   = std::shared_ptr<Cascade>;
    using ref_w = std::weak_ptr<Cascade>; 
    using idf = std::size_t; 
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

  inline std::string get_string_val(std::istream& is);
  inline std::istream& operator>>(std::istream& is, tweet& t);
  
  //Comparator for the Binomial_heap queue
  struct cascade_ref_comparator {
    bool operator()(cascade::ref op1, cascade::ref op2) const; 
  };
  //Priority queue type
  using priority_queue = boost::heap::binomial_heap<cascade::ref,
                            boost::heap::compare<cascade_ref_comparator>>;
  
  struct Cascade {
    cascade::idf key;         // the id of the original tweet
    source::idf source_id;    // the id if the source of the tweet
    std::string msg;          // msg of the tweet
    timestamp latest_t;    // the time of the newest retweet
    timestamp first_t;     // the starting time of the cascade
    std::vector<std::pair<timestamp,double>> tweets;  // Storing only times and magnitudes
    priority_queue::handle_type location; //this is needed to update the priority queue

    bool operator<(const Cascade& other) const;

    Cascade() = default;
    // A cascade can be fully constructed from a tweet 
    Cascade(const tweet& t) : key(t.key),source_id(t.source),msg(t.msg),latest_t(t.time),
    first_t(t.time){
      tweets.push_back({t.time,t.magnitude});
    };

    virtual ~Cascade() {};

    friend std::ostream& operator<<(std::ostream& os, const Cascade& c);
  };

  // Convenient way to create a cascade and make a shared pointer of it
  cascade::ref cascade_ptr(const tweet& t);

  std::ostream& operator<<(std::ostream& os, const Cascade& c);
  std::string format_cascade_series(const Cascade& c, timestamp obs); //msg to send to topic cascade_series
  std::string format_cascade_properties(const Cascade& c); //msg to send to topic cascade_properties
  void send_kafka(cascade::ref c_ptr,Processors& pr, timestamp obs, bool Terminated);
  //Processor manages cascades for tweets comming from the same source
  struct Processor {
    params::collector parameters;
    //queue for handling cascades termination
    priority_queue cascade_queue;
    //Map for handling partial cascade
    std::map<timestamp, std::queue<cascade::ref_w>> partial_cascade;
    // symbol table
    std::map<cascade::idf, cascade::ref_w> symbols;

    Processor() = default;
    // A given processor is constructed from a tweet and parameters 
    Processor(const tweet& t, params::collector p);
    virtual ~Processor() {};

  };

  struct Processors {
    // a map containing all the processors (every processor is associated with a source)
    std::map<source::idf, Processor> processors;
    // parameters from the config file
    params::collector parameters;
    // Producer as a member of Processors, to avoid to create several producers each time sending a kafka message
    cppkafka::Configuration config;
    cppkafka::Producer  producer;
    Processors() = default;
    Processors(const std::string& config_filename) : parameters(config_filename),
    config({{"metadata.broker.list", parameters.kafka.brokers},
      {"log.connection.close", false }}),producer(config) {};

    virtual ~Processors() {};

    // This removes a processor
    void operator-=(const source::idf& s);

    void operator+=(const tweet& t); //This operator handles adding tweets to given cascades in the processors map, creating a new processor for new sources and sending terminated and partial cascades

    
  }; 
}
