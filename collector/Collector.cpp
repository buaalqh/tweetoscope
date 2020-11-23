#pragma once
#include "Collector.hpp"

namespace tweetoscope {

inline bool Cascade::operator<(const Cascade& other) const {
        return obs < other.obs;
} 

bool cascade_ref_comparator::operator()(cascade::ref op1, cascade::ref op2) const{
      return *op1 < *op2;
}

std::ostream& operator<<(std::ostream& os, const Cascade& c) {
    // print cascade
      os << "{\"cascade_id\" : "        << c.cid         << " , "
        << "\"source_id\" : "   << c.source_id   << " , "
        << "\"msg\" : "         << c.msg         << " , "
        << "\"observation_time\" : " << c.obs << " , "
        << "\"list_retweets\" : [";

      for(auto& [ti,mi] : c.tweets){
        os << "(\"time\": " << ti << " , " << "\"magnitude\": " << mi << "\"),";}
        os << "]}";     
      return os;
}

std::string msg_cascade_series(const Cascade& c, timestamp obs) {
        //Formating the message to send to kafka topic cascade_series
        //Key = None Value = { 'type' : 'serie', 'cid': 'tw23981', 'msg' : 
        //'blah blah', 'T_obs': 600, 'tweets': [ (t1, m1), (t2,m2), ... ] }
        std::ostringstream os;
        os << "{\'type\' : "  << "\'serie\'"  << " , "
        << "\'cid\' : "    << c.cid        << " , "
        << "\'msg\' : "    << c.msg        << " , "
        << "\'T_obs\' : "  << obs          << " , " 
        << "\'tweets\' : [";
        for(auto ptr_t = c.tweets.begin(); ptr_t != c.tweets.end(); ++ptr_t){
        os << "(" << ptr_t->first << ", " << ptr_t->second << ")";
        if (ptr_t != c.tweets.end()-1) os << ",";
        }
        os << "]}";
        return os.str();
}

std::string msg_cascade_properties(const Cascade& c) {
    // Key = 300  Value = { 'type' : 'size', 'cid': 'tw23981', 'n_tot': 127, 't_end': 4329 }
        std::ostringstream os;
        os << "{\'type\' : "  << "\'size\'"    << " , "
        << "\'cid\' : "    << c.cid         << " , "
        << "\'n_tot\' : "  << c.tweets.size() << " , "
        << "\'t_end\' : "  << c.obs << "}";
        return os.str();
}

// Send debug Message to Kafka topic logs about partial cascade:
void send_partial_to_logs(const std::size_t& window_first ,tweetoscope::cascade::ref sp, 
cppkafka::MessageBuilder& builder_logs, cppkafka::Producer& producer, const tweetoscope::params::collector& params){

	std::ostringstream ostr;
	ostr 	<< "Cascade " << sp->cid 
			<< " with time window " << window_first 
			<< " was sent out to " << params.topic.out_series 
			<< ".";

	//std::cout << std::endl << ostr.str() << std::endl << std::endl;

	std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
	std::time_t now_c = std::chrono::system_clock::to_time_t(now);									

	std::ostringstream ostr2;
	ostr2	<< 	"{\"t\": "	<< now_c
			<< ", \"source\": \"Collector\", \"level\": \"DEBUG\", \"message\": \""
			<<	ostr.str()
			<<	"\"}";
	
	builder_logs.key(params.topic.out_logs);
	auto msg_out = ostr2.str();
	builder_logs.payload(msg_out);
	producer.produce(builder_logs);
	producer.flush();
}
//Send Debug Message to topic logs about cascade terminaison:
void send_terminaison_to_logs(const std::size_t& observation_time ,tweetoscope::cascade::ref sp_top, 
cppkafka::MessageBuilder& builder_logs, cppkafka::Producer& producer, const tweetoscope::params::collector& params){


	std::string	key_str = std::to_string(static_cast<int>(observation_time));
	std::ostringstream ostr;
	ostr 	<< "Cascade " 							<< sp_top->cid
			<< " was terminated and sent out to " 	<< params.topic.out_properties
			<< " with key "							<< key_str 
			<< ".";

	//std::cout << std::endl << ostr.str() << std::endl << std::endl;
	std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
	std::time_t now_c = std::chrono::system_clock::to_time_t(now);									

	std::ostringstream ostr2;
	ostr2	<< 	"{\"t\": "	<< now_c
			<< ", \"source\": \"Collector\", \"level\": \"DEBUG\", \"message\": \""
			<<	ostr.str()
			<<	"\"}";
	
	builder_logs.key(params.topic.out_logs);
	auto msg_out = ostr2.str();
	builder_logs.payload(msg_out);
	producer.produce(builder_logs);
	producer.flush();
}


void producer_kafka(cascade::ref c_ptr, Processor_map& pr, timestamp obs, bool Terminated){
      if (Terminated) { 
        cppkafka::MessageBuilder builder_p {pr.parameters.topic.out_properties};
        cppkafka::MessageBuilder builder_logs {pr.parameters.topic.out_logs};
        auto key = std::to_string(obs);
        builder_p.key(key);
        auto msg = msg_cascade_properties(*c_ptr);
        builder_p.payload(msg);
        pr.producer.produce(builder_p);        
        send_terminaison_to_logs(obs, c_ptr,builder_logs, pr.producer, pr.parameters);
      } else {
        cppkafka::MessageBuilder builder_s {pr.parameters.topic.out_series};
        cppkafka::MessageBuilder builder_logs {pr.parameters.topic.out_logs};
        builder_s.key();
        auto msg = msg_cascade_series(*c_ptr,obs);
        builder_s.payload(msg);
        pr.producer.produce(builder_s);
        send_partial_to_logs(obs,c_ptr, builder_logs,pr.producer,pr.parameters);
      }

    }
    // function to create a cascade and make share pointer
    cascade::ref cascade_ptr(const tweet& t) {
      return std::make_shared<Cascade>(std::move(t));
    }
    Processor::Processor(const tweet& t, params::collector p) : parameters(p) {
    auto r = cascade_ptr(t);
    r->location = cascade_queue.push(r);
    for (auto obs : p.times.observation) {
      partial_cascade.insert({obs, {}});
      partial_cascade[obs].push(r);
    }
    locations.insert(std::make_pair(t.key, r));
    }
  
inline void Processor_map::operator-=(const source::idf& s) {
          if(auto it = processors.find(s); it != processors.end())
          processors.erase(it);
}

inline void Processor_map::operator+=(const tweet& t) {
        //Case 1: Source not exist
        auto [p_ptr,is_new_source] = processors.try_emplace(t.source,t,this->parameters);
        // Case 2: Source exists
        if (!is_new_source) {
          auto c_ptr = cascade_ptr(t);
          // ckeck cid is in the locations table, if not we insert it 
          auto [it_s, is_location_created] = p_ptr->second.locations.insert(std::make_pair(t.key, c_ptr));

          // Partial cascades 
          for(auto& [obs, cascades]: p_ptr->second.partial_cascade){
            while(!cascades.empty()) {
              if (auto sp = cascades.front().lock()) {
                if (t.time - sp->start_t > obs) {
                  // send the kafka : topic = cascade_series
                  producer_kafka(sp, *this, obs, false);
                  cascades.pop();
                } else break; 
              } else cascades.pop(); //delete cascades which are terminated but the weak_pointer is still present in the partial_cascade map
            }
            // new created cascade should be added to all the partial cascades
            if(is_location_created) cascades.push(c_ptr);
          }

          ///Queue
          // Send the kafka message and erase the terminated cascades from the queue
          while(!p_ptr->second.cascade_queue.empty() && this->parameters.times.terminated \
               < t.time - p_ptr->second.cascade_queue.top()->obs) {
            
            auto r = p_ptr->second.cascade_queue.top();
            for (auto obs : this->parameters.times.observation) {
                // send the kafka : topic = cascade_properties
                producer_kafka(r, *this, obs, true);
            }
          p_ptr->second.cascade_queue.pop();
          }
        // Push new cascade it to the queue
        if (is_location_created) c_ptr->location = p_ptr->second.cascade_queue.push(c_ptr);

        //Update queue
        if(auto sp = it_s->second.lock()) {
          sp->obs = t.time;
          // Only push the tweet if the cascade is not newly created
          if (!is_location_created) sp->tweets.push_back(std::make_pair(t.time,t.magnitude)); 
          // Update the location in the queue (when tweet is added the priority of the cascade changes)
          p_ptr->second.cascade_queue.update(sp->location);
        }
        }

    }

}
