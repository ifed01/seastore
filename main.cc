#include "core/sleep.hh"
#include "core/app-template.hh"
#include "core/reactor.hh"
#include "core/gate.hh"
#include <iostream>
#include <pthread.h>
#include <string>
#include <queue>
using namespace std::chrono_literals;

thread_local bool stop_flag = false;;
thread_local seastar::server_socket* common_listener = nullptr; 

class Req;
typedef seastar::shared_ptr<Req> ReqPtr;

class Req {
public:
  Req(const char* _name, int _col_id, std::chrono::milliseconds _wait_ms) : 
    name(_name), col_id(_col_id), wait_ms(_wait_ms) {
  }
  virtual ~Req() {
    std::cout<<"~"<<name.c_str()<<std::endl;
  }
  std::string name;
  int col_id;
  std::chrono::milliseconds wait_ms;
  seastar::future<> process() {
    std::cout<<"processing:"<<name.c_str()<<std::endl;
    return 
      seastar::sleep(wait_ms).then([this] {  
        std::cout<<"processed:"<<name.c_str()<<std::endl;
        return seastar::make_ready_future<>();
    });
  }

};

class Q {
  std::queue<std::pair<seastar::promise<>, ReqPtr>> q;
  bool waiting = false;
  std::string name;
  seastar::semaphore limit;
public:
  Q(const char* _name = "") : name(_name), limit(1) {
  }
  Q(Q&& q) : 
    q(std::move(q.q)),
    waiting(q.waiting),
    name(std::move(q.name)),
    limit(std::move(q.limit)) {
  }
  virtual ~Q() {
    std::cout<<"~"<<name.c_str()<<std::endl;
  }
  bool is_empty() const {
    return !waiting && q.size() == 0;
  }
  void set_name(const char* _name) {
    name = _name;
  }
  const char* get_name() const {
    return name.c_str();
  }

  seastar::future<> _queue2(seastar::promise<>&& p, ReqPtr r) {
    waiting = true;
    return with_semaphore(limit, 1, [r, this, p = std::move(p)] () mutable {
      return do_with(ReqPtr(r), [this, p = std::move(p)] (auto&r) mutable {
         return r->process().then([this, p = std::move(p)] () mutable {
           waiting = false;
           p.set_value();
           if (q.size()) {
             _queue2(std::move(q.front().first), q.front().second);
             q.pop();
           }
           return seastar::make_ready_future<>();
         });
      });
    });
  }
  seastar::future<> queue2(ReqPtr r) {
    std::cout<<"queue2:"<<r->name.c_str()<<std::endl;
    if (q.size() || waiting) {
      q.emplace(seastar::promise<>(), r);
      return q.back().first.get_future();
    }
    return _queue2(seastar::promise<>(), r); // fake promise
  } 

  seastar::future<ReqPtr> queue(ReqPtr r) {
    std::cout<<"queue:"<<r->name.c_str()<<std::endl;
    return with_semaphore(limit, 1, [r] {
      return r->process().then([r] {
        return seastar::make_ready_future<ReqPtr>(r);
      });
    });
  } 
  seastar::future<> queue_and_forget(ReqPtr r) {
    std::cout<<"queue:"<<r->name.c_str()<<std::endl;
    return with_semaphore(limit, 1, [r] {
      return do_with(ReqPtr(r), [] (auto&r) {
         return r->process().then([] {
           return seastar::make_ready_future<>();
         });
      });
    });
  }
  template <typename Func> 
  seastar::future<> queue_fn(ReqPtr r,
                             Func&& fn) {
    std::cout<<"queue_fn:"<<r->name.c_str()<<std::endl;
    return with_semaphore(limit, 1, [r, fn] {
      return do_with(ReqPtr(r), [fn] (auto&r) {
         return fn(r).then([] {
           return seastar::make_ready_future<>();
         });
      });
    });
  }
};
typedef seastar::shared_ptr<Q> QPtr;

class StoreShard;
typedef seastar::shared_ptr<StoreShard> StoreShardPtr;

class StoreShard
{
  std::map<int, QPtr> col_queues;
public:
  StoreShard()
  {
  }

  virtual ~StoreShard() {
    std::cout<<"~StoreShard:"<<this<<std::endl;
  }
  seastar::future<> distribute(ReqPtr r) {
    if (stop_flag) {
      throw std::string("termination is in progress");
    }
    auto it = col_queues.find(r->col_id);
    if (it == col_queues.end()) {
      char buf[32];
      sprintf(buf, "que_%u", r->col_id);
      it = col_queues.emplace(r->col_id, seastar::make_shared<Q>(buf)).first;
    }
    std::cout<<"forward "<<r->name.c_str()<<" to "<<this<<":"<<it->second->get_name()<<std::endl; 
    return it->second->queue2(r).finally([this, col_id = r->col_id] {
      std::cout<<"RemoveQueue?"<<col_id<<std::endl;
      auto it = col_queues.find(col_id);
      if (it != col_queues.end() &&
          it->second->is_empty()) {
        col_queues.erase(it);
        std::cout<<"RemoveQueue!"<<col_id<<std::endl;
      }
    });
  }
  seastar::future<> loop() {
    return seastar::repeat([this] {
      return seastar::sleep(500ms).then([this] {
        return stop_flag && col_queues.empty() ?
            seastar::stop_iteration::yes :
            seastar::stop_iteration::no;
      });
    });
  }
  
  static thread_local StoreShardPtr thread_shard;
  static seastar::future<> run() {
    assert(thread_shard.get() == nullptr);
    thread_shard = seastar::make_shared<StoreShard>();
    return thread_shard->loop();
  }
  static seastar::future<> process(ReqPtr r) {
    int shard_no = r->col_id % seastar::smp::count;
std::cout<<"process submitting to shard="<<shard_no<<std::endl;
    return seastar::smp::submit_to( shard_no, [r] () {
      assert(thread_shard.get() != nullptr);
      return thread_shard->distribute(r);
    });
  }
};
thread_local StoreShardPtr StoreShard::thread_shard;

seastar::future<> q_test() {
    return seastar::do_with(Q ("q"), [] (auto& q) {
      ReqPtr r1 = seastar::make_shared<Req>("first_1", 1, 2000ms);
      ReqPtr r2 = seastar::make_shared<Req>("second_2", 2, 3000ms);
      ReqPtr r3 = seastar::make_shared<Req>("third_1", 1, 1000ms);
      ReqPtr r4 = seastar::make_shared<Req>("forth_2", 2, 3000ms);
      auto f1 = q.queue2(r1);
      auto f2 = q.queue2(r2);
      auto f3 = q.queue2(r3);
      auto f4 = q.queue2(r4);
      return seastar::when_all(std::move(f1), std::move(f2), std::move(f3), std::move(f4))
        .then( [] (auto t) {
          std::cout<<"all done"<<std::endl;
          return seastar::make_ready_future<>();
        });
    });

}

seastar::future<> StoreShardTest()
{
    StoreShardPtr s = seastar::make_shared<StoreShard>();
    return seastar::do_with(StoreShardPtr(s), [] (auto& s) {
      ReqPtr r1 = seastar::make_shared<Req>("first_1", 1, 2000ms);
      ReqPtr r2 = seastar::make_shared<Req>("second_2", 2, 3000ms);
      ReqPtr r3 = seastar::make_shared<Req>("third_1", 2, 1000ms);
      ReqPtr r4 = seastar::make_shared<Req>("forth_2", 1, 3000ms);

      auto f1 = s->distribute(r1);
      auto f2 = s->distribute(r2);
      auto f3 = s->distribute(r3);
      auto f4 = s->distribute(r4);
      return seastar::when_all(std::move(f1), std::move(f2), std::move(f3), std::move(f4))
        .then( [] (auto t) {
          std::cout<<"all done for shard"<<std::endl;
          return seastar::make_ready_future<>();
      });
    });
}

seastar::future<> handle_connection(seastar::connected_socket s,
                                    seastar::socket_address a) {
    auto out = s.output();
    auto in = s.input();
    std::cout<<"handling con..."<<std::endl;
    return do_with(std::move(s), std::move(out), std::move(in),
        [] (auto& s, auto& out, auto& in) {
            return seastar::repeat([&out, &in] {
                return in.read().then([&out] (auto buf) {
                    if (buf) {
                        const char* cptr = buf.get();
                        if (strstr(cptr, "stop") == cptr) {
                          std::cout<<"stop received"<<std::endl;
                          seastar::smp::invoke_on_all([] {
                            stop_flag = true;
                            if (common_listener) {
                              common_listener->abort_accept();
                            } 
                            return seastar::make_ready_future<>();
                          });
                          return seastar::make_ready_future<seastar::stop_iteration>(
                            seastar::stop_iteration::no);
                        } else if (strstr(cptr, "req") == cptr) {
                          std::string str(cptr + 3);
                          char* s = &str.at(0);
                          char* name = std::strtok(s, " \t");
                          long col_id = std::strtol(
                            std::strtok(nullptr," \t"),
                            nullptr, 10);
                          long ms_long = std::strtol(
                            std::strtok(nullptr," \t"),
                            nullptr, 10);
                          std::chrono::milliseconds ms(ms_long);
                          ReqPtr r = seastar::make_shared<Req>(name, col_id, ms);
                          StoreShard::process(r);
                          return seastar::make_ready_future<seastar::stop_iteration>(
                            seastar::stop_iteration::no);
                        }
                        std::cout<<"<<"<<buf.get()<<std::flush;
                        return out.write(std::move(buf)).then([&out] {
                            return out.flush();
                        }).then([] {
                            return seastar::stop_iteration::no;
                        });
                    } else {
                        return seastar::make_ready_future<seastar::stop_iteration>(
                            seastar::stop_iteration::yes);
                    }
                });
            }).then([&out] {
                std::cout<<"closing con..."<<std::endl;
                return out.close();
            });
        });
}

seastar::future<> ip_listen_loop() {
    seastar::listen_options lo;
    lo.reuse_address = true;
    return seastar::do_with(seastar::gate(), seastar::listen(seastar::make_ipv4_address({1234}), lo),
        [] (auto &g, auto& listener) {
        common_listener = &listener;
	return  seastar::repeat([&g, &listener] {
          if (stop_flag) {
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::yes);
          }
          //std::cout<<"repeat"<<pthread_self()<<std::endl;
          return listener.accept().then([&g, &listener]
                   (seastar::connected_socket s, seastar::socket_address a) {
//std::cout<<"post accept "<<pthread_self()<<std::endl;
            seastar::with_gate(g, [&s, &a] {
              handle_connection(std::move(s), std::move(a));
            });
//std::cout<<"post gate"<<std::endl;
            return seastar::stop_iteration::no;
          }).handle_exception([](...) {
            std::cout<<"exception while handling connection"<<std::endl;
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::yes);
          });
        }).then( [&g] {
//std::cout<<"close gate"<<std::endl;
          return g.close();
        });
    }).finally([] {
      common_listener = nullptr;
      return seastar::make_ready_future<>();
    });
}

boost::integer_range<unsigned> effective_shards(0, 1);
int main(int argc, char** argv) {
  seastar::app_template app;
  app.run(argc, argv, [] {
    effective_shards = seastar::smp::all_cpus();
    std::cout<<"RUN: "<<effective_shards<< " shards/cpus .."<<std::endl;
    auto f0 = seastar::smp::invoke_on_all([] {
      return StoreShard::run();
    });
    auto f1 = seastar::smp::invoke_on_all([] {
      return ip_listen_loop();
    });
    return seastar::when_all(std::move(f0), std::move(f1)).then([] (auto t) {
      std::cout<<"all done"<<std::endl;
      return seastar::make_ready_future<>();
    });
  });
}
