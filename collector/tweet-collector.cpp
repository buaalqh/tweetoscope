/*   g++ -o tweet-collector tweet-collector.cpp -O3 $(pkg-config --cflags --libs gaml) -lpthread -lcppkafka -lboost_system
  ./kafka.sh start --zooconfig zookeeper.properties --serverconfig server.properties 
  ./kafka.sh stop */

#include "Collector.cpp"
#include "tweetoscopeCollectorParams.hpp"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <queue>
#include <thread>
#include <atomic>
#include <sstream>
#include <stdexcept>
#include <iterator>
#include <cppkafka/cppkafka.h>
#include <boost/heap/binomial_heap.hpp>
#include <map>
#include <utility>
#include <string>
#include <vector>
#include <tuple>

int main(int argc, char* argv[]) {


  if(argc != 2) {
    std::cout << "Usage : " << argv[0] << " <config-filename>" << std::endl;
    return 0;
  }


  tweetoscope::params::collector params(argv[1]);
  std::cout << std::endl
        << "Parameters : " << std::endl
        << "----------"    << std::endl
        << std::endl
        << params << std::endl
        << std::endl;



  // Create the kafka consumer
	cppkafka::Configuration config {
	  {"metadata.broker.list", params.kafka.brokers},
    { "auto.offset.reset", "earliest" },
	  {"log.connection.close", false },
    {"group.id","mygroup"}
	};

	cppkafka::Consumer consumer(config);
  
  
  consumer.subscribe({params.topic.in});

  //Create a Processor Map 
  tweetoscope::Processor_map processors(argv[1]);


  while(true) {
    auto msg = consumer.poll();
    if( msg && ! msg.get_error() ) {
       tweetoscope::tweet twt;
       auto key = tweetoscope::cascade::idf(std::stoi(msg.get_key()));
       auto istr = std::istringstream(std::string(msg.get_payload()));
       istr >> twt;
       twt.key = key;
       processors += twt;
       //consumer.commit(msg);
    }
}

  return 0;
}

