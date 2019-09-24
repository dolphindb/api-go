#ifndef _STREAMING_H_
#define _STREAMING_H_
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "Concurrent.h"
#include "DolphinDB.h"

namespace dolphindb {

template <typename T>
class BlockingQueue;

using Message = VectorSP;
using MessageQueue = BlockingQueue<Message>;
using MessageQueueSP = SmartPointer<MessageQueue>;
using MessageHandler = std::function<void(Message)>;
using std::unordered_multimap;

const int DEFAULT_QUEUE_CAPACITY = 65536;
const string DEFAULT_ACTION_NAME = "cppStreamingAPI";

template <typename T>
class BlockingQueue {
   public:
    BlockingQueue(size_t maxItems) : capacity_(maxItems), size_(0), head_(0), tail_(0) { buf_ = new T[maxItems]; }

    ~BlockingQueue() { delete[] buf_; }

    void push(const T& item) {
        lock_.lock();
        while (size_ >= capacity_) full_.wait(lock_);
        buf_[tail_] = item;
        tail_ = (tail_ + 1) % capacity_;
        ++size_;

        if (size_ == 1) empty_.notifyAll();
        lock_.unlock();
    }

    bool poll(T& item, int milliSeconds) {
        if (milliSeconds < 0) {
            pop(item);
            return true;
        }
        LockGuard<Mutex> guard(&lock_);
        while (size_ == 0) {
            if (!empty_.wait(lock_, milliSeconds)) return false;
        }
        item = buf_[head_];
        buf_[head_] = T();
        head_ = (head_ + 1) % capacity_;
        --size_;

        if (size_ == capacity_ - 1) full_.notifyAll();
        return true;
    }

    void pop(T& item) {
        lock_.lock();
        while (size_ == 0) empty_.wait(lock_);

        item = buf_[head_];
        buf_[head_] = T();
        head_ = (head_ + 1) % capacity_;
        --size_;

        if (size_ == capacity_ - 1) full_.notifyAll();
        lock_.unlock();
    }

   private:
    T* buf_;
    size_t capacity_;
    size_t size_;
    size_t head_;
    size_t tail_;
    Mutex lock_;
    ConditionalVariable full_;
    ConditionalVariable empty_;
};

string getIOERRStr(IO_ERR);

class Executor : public Runnable {
    using Func = std::function<void()>;

   public:
    explicit Executor(Func f) : func_(std::move(f)){};
    void run() override { func_(); };

   private:
    Func func_;
};

class StreamingClient {
    struct SubscribeInfo {
        string host;
        int port;
        string tableName;
        string actionName;
        long long offset = -1;
        bool resub;
        VectorSP filter;
    };

   public:
    explicit StreamingClient(int listeningPort);
    virtual ~StreamingClient();

   protected:
    MessageQueueSP subscribeInternal(string host,
                                     int port,
                                     string tableName,
                                     string actionName = DEFAULT_ACTION_NAME,
                                     int64_t offset = -1,
                                     bool resubscribe = true,
                                     VectorSP filter = nullptr);

    void unsubscribeInternal(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);

   private:
    void daemon();
    void parseMessage(SocketSP socket);
    string getLocalHostname(string remoteHost, int remotePort);
//    MessageQueueSP resubscribe(string topic, long long offset);
    void resubscribe(string site);
    string stripActionName(string topic);
    string getSite(string topic);
    void increase(string topic);
    void decrease(string topic);

   private:
    SocketSP listenerSocket_;
    ThreadSP daemonThread_;
    vector<ThreadSP> parseThreads_;
    int listeningPort_;
    string localIP_;
    unordered_map<string, SubscribeInfo> saveSub_;
    unordered_map<string, long long> topicOffset;
    unordered_map<string, MessageQueueSP> queues_;
    unordered_map<string, int> actionCntOnTable_;
    unordered_multimap<string, string> liveSubsOnSite_; // living site -> topic
    Mutex saveSubMutex_;
    Mutex liveSubsOnSiteMutex_;
    Mutex queueMutex_; // allow only one thread write queue_
    Mutex actionCntOnTableMutex_;
    Mutex subUnsubMutex_;
#ifdef WINDOWS
    static bool WSAStarted_;
    static void WSAStart();
#endif
};

class ThreadedClient : private StreamingClient {
   public:
    explicit ThreadedClient(int listeningPort);
    ~ThreadedClient() override;
    ThreadSP subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
    void unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
};

class ThreadPooledClient : private StreamingClient {
   public:
    explicit ThreadPooledClient(int listeningPort, int threadCount);
    ~ThreadPooledClient() override;
    vector<ThreadSP> subscribe(string host, int port, MessageHandler handler, string tableName, string actionName, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
    void unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);

   private:
    int threadCount_;
};

class PollingClient : private StreamingClient {
   public:
    explicit PollingClient(int listeningPort);
    ~PollingClient() override;
    MessageQueueSP subscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
    void unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
};

}  // namespace dolphindb
#endif  // _STREAMING_H_