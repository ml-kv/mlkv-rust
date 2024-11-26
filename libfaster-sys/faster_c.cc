// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.


#include "faster_c.h"
#include "core/faster.h"
#include "device/file_system_disk.h"
#include "device/null_disk.h"

extern "C" {

using namespace FASTER::core;

class Key {
  public:
    Key(uint64_t key)
      : key_{ key } {
    }

    /// Methods and operators required by the (implicit) interface:
    inline static constexpr uint32_t size() {
      return static_cast<uint32_t>(sizeof(Key));
    }
    inline FASTER::core::KeyHash GetHash() const {
      return FASTER::core::KeyHash{ FASTER::core::Utility::GetHashCode(key_) };
    }

    /// Comparison operators.
    inline bool operator==(const Key& other) const {
      return key_ == other.key_;
    }
    inline bool operator!=(const Key& other) const {
      return key_ != other.key_;
    }

  private:
    uint64_t key_;
};

class GenLock {
  public:
    GenLock()
      : control_{ 0 } {
    }
    GenLock(uint64_t control)
      : control_{ control } {
    }
    inline GenLock& operator=(const GenLock& other) {
      control_ = other.control_;
      return *this;
    }

    union {
      struct {
        int32_t staleness : 32;
        uint64_t gen_number : 30;
        uint64_t locked : 1;
        uint64_t replaced : 1;
      };
      uint64_t control_;
    };
};

class AtomicGenLock {
  public:
    AtomicGenLock()
      : control_{ 0 } {
    }
    AtomicGenLock(uint64_t control)
      : control_{ control } {
    }

    inline GenLock load() const {
      return GenLock{ control_.load() };
    }
    inline void store(GenLock desired) {
      control_.store(desired.control_);
    }

    inline bool try_lock(bool& replaced, int32_t staleness_incr, int32_t staleness_bound) {
      replaced = false;
      GenLock expected{ control_.load() };
      expected.locked = 0;
      expected.replaced = 0;
      GenLock desired{ expected.control_ };
      desired.locked = 1;
      desired.staleness = expected.staleness + staleness_incr;

      if (desired.staleness > staleness_bound) {
        return false;
      }

      if(control_.compare_exchange_strong(expected.control_, desired.control_)) {
        return true;
      }
      if(expected.replaced) {
        replaced = true;
      }
      return false;
    }
    inline void unlock(bool replaced) {
      if(!replaced) {
        // Just turn off "locked" bit and increase gen number.
        uint64_t sub_delta = ((uint64_t)1 << 62) - ((uint64_t)1 << 32);
        control_.fetch_sub(sub_delta);
      } else {
        // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
        uint64_t add_delta = ((uint64_t)1 << 63) - ((uint64_t)1 << 62) + ((uint64_t)1 << 32);
        control_.fetch_add(add_delta);
      }
    }

  private:
    std::atomic<uint64_t> control_;
};

class Value {
  public:
    Value()
      : gen_lock_{ 0 }
      , size_{ 0 }
      , length_{ 0 } {
    }

    inline uint32_t size() const {
      return size_;
    }

    friend class UpsertContext;
    friend class ReadContext;
    friend class RmwContext;
    friend class MLKVReadContext;
    friend class MLKVUpsertContext;
    friend class MLKVLookaheadContext;

  private:
    AtomicGenLock gen_lock_;
    uint64_t size_;
    uint64_t length_;

    inline const uint8_t* buffer() const {
      return reinterpret_cast<const uint8_t*>(this + 1);
    }
    inline uint8_t* buffer() {
      return reinterpret_cast<uint8_t*>(this + 1);
    }
};

class ReadContext : public IAsyncContext {
  public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint64_t key, uint8_t* output)
      : key_{ key }
      , output_ { output }  {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
      : key_{ other.key_ }
      , output_ { other.output_ }  {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }

    inline void Get(const value_t& value) {
      // All reads should be atomic (from the mutable tail).
      // assert(false);
      // TODO: make sure the correctness disk-based operations
      std::memcpy(output_, value.buffer(), value.length_);
    }

    inline void GetAtomic(const value_t& value) {
      GenLock before, after;
      do {
        before = value.gen_lock_.load();
        std::memcpy(output_, value.buffer(), value.length_);
        after = value.gen_lock_.load();
      } while(before.gen_number != after.gen_number);
    }

  protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

  private:
    key_t key_;
    uint8_t* output_;
  };

class UpsertContext : public IAsyncContext {
  public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint64_t key, uint8_t* input, uint64_t length)
      : key_{ key }
      , input_{ input }
      , length_{ length } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(UpsertContext& other)
      : key_{ other.key_ }
      , input_{ other.input_ }
      , length_{ other.length_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + length_;
    }
    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t& value) {
      value.gen_lock_.store(0);
      value.size_ = sizeof(value_t) + length_;
      value.length_ = length_;
      std::memcpy(value.buffer(), input_, length_);
    }
    inline bool PutAtomic(Value& value) {
      bool replaced;
      while(!value.gen_lock_.try_lock(replaced, /*staleness_incr*/ 0, /*staleness_bound*/ INT32_MAX)
            && !replaced) {
        std::this_thread::yield();
      }
      if(replaced) {
        // Some other thread replaced this record.
        return false;
      }
      if(value.size_ < sizeof(Value) + length_) {
        // Current value is too small for in-place update.
        value.gen_lock_.unlock(true);
        return false;
      }
      // In-place update overwrites length and buffer, but not size.
      value.length_ = length_;
      std::memcpy(value.buffer(), input_, length_);
      value.gen_lock_.unlock(false);
      return true;
    }

  protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

  private:
    key_t key_;
    uint8_t* input_;
    uint64_t length_;
};

class RmwContext : public IAsyncContext {
  public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(uint64_t key, uint8_t* incr, uint64_t length)
      : key_{ key }
      , incr_{ incr }
      , length_{ length } {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext& other)
      : key_{ other.key_ }
      , incr_{ other.incr_ }
      , length_{ other.length_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const key_t& key() const {
      return key_;
    }
    inline uint32_t value_size() const {
      return sizeof(value_t) + length_;
    }
    inline uint32_t value_size(const value_t& old_value) const {
      return sizeof(value_t) + length_;
    }

    inline void RmwInitial(value_t& value) {
      assert(false);
    }
    inline void RmwCopy(const value_t& old_value, value_t& value) {
      assert(false);
    }
    inline bool RmwAtomic(value_t& value) {
      bool replaced;
      while(!value.gen_lock_.try_lock(replaced, /*staleness_incr*/ 0, /*staleness_bound*/ INT32_MAX)
            && !replaced) {
        std::this_thread::yield();
      }
      if(replaced) {
        // Some other thread replaced this record.
        return false;
      }
      // In-place update overwrites length and buffer, but not size.
      value.length_ = length_;
      for (int32_t idx = 0; idx < std::min(value.length_, length_) / sizeof(uint64_t); ++idx) {
        (static_cast<uint64_t *>((void *)value.buffer()))[idx] += (static_cast<uint64_t *>((void *)incr_))[idx];
      }
      value.gen_lock_.unlock(false);
      return true;
    }

  protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

  private:
    key_t key_;
    uint8_t* incr_;
    uint64_t length_;
};

class DeleteContext : public IAsyncContext {
  public:
      typedef Key key_t;
      typedef Value value_t;

      DeleteContext(uint64_t key)
      : key_{ key } {

      }

      /// Copy (and deep-copy) constructor.
      DeleteContext(DeleteContext& other)
      : key_ { other.key_ } {
      }

      /// The implicit and explicit interfaces require a key() accessor.
      inline const key_t& key() const {
        return key_;
      }
      inline uint32_t value_size() const {
        return sizeof(value_t);
      }

  protected:
      /// The explicit interface requires a DeepCopy_Internal() implementation.
      Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }

  private:
      key_t key_;
};

class MLKVReadContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  MLKVReadContext(uint64_t key, uint8_t* output, uint64_t length,
                  int32_t staleness_incr, int32_t staleness_bound)
    : key_{ key }
    , output_{ output }
    , length_{ length }
    , staleness_incr_{ staleness_incr }
    , staleness_bound_{ staleness_bound } {
  }

  /// Copy (and deep-copy) constructor.
  MLKVReadContext(const MLKVReadContext& other)
    : key_{ other.key_ }
    , output_{ other.output_ }
    , length_{ other.length_ }
    , staleness_incr_{ other.staleness_incr_ }
    , staleness_bound_{ other.staleness_bound_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  const key_t& key() const {
    return key_;
  }
  inline int32_t value_size() const {
    return sizeof(value_t) + length_;
  }
  inline uint32_t value_size(const value_t& old_value) const {
    return sizeof(value_t) + length_;
  }

  /// Initial, non-atomic, and atomic RMW methods.
  inline void RmwInitial(value_t& value) {
    // assert(false);
    found = false;
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    GenLock before, after;
    before = old_value.gen_lock_.load();
    after.staleness = before.staleness + staleness_incr_;

    value.gen_lock_.store(after);
    value.size_ = sizeof(value_t) + length_;
    value.length_ = length_;

    std::memcpy(value.buffer(), old_value.buffer(), old_value.length_);
    std::memcpy(output_, old_value.buffer(), old_value.length_);
    found = true;
  }
  inline bool RmwAtomic(value_t& value) {
    bool replaced;
    while(!value.gen_lock_.try_lock(replaced, staleness_incr_, staleness_bound_)
          && !replaced) {
      std::this_thread::yield();
    }
    if(replaced) {
      // Some other thread replaced this record.
      return false;
    }
    if(value.size_ < sizeof(value_t) + length_) {
      // Current value is too small for in-place update.
      value.gen_lock_.unlock(true);
      return false;
    }
    // In-place update overwrites length and buffer, but not size.
    value.length_ = length_;
    std::memcpy(output_, value.buffer(), value.length_);
    value.gen_lock_.unlock(false);
    found = true;
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 public:
  bool found;

 private:
  key_t key_;
  uint8_t* output_;
  uint64_t length_;
  int32_t staleness_incr_;
  int32_t staleness_bound_;
};

class MLKVUpsertContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  MLKVUpsertContext(uint64_t key, uint8_t* input, uint64_t length,
                    int32_t staleness_incr, int32_t staleness_bound)
    : key_{ key }
    , input_{ input }
    , length_{ length }
    , staleness_incr_{ staleness_incr }
    , staleness_bound_{ staleness_bound } {
  }

  /// Copy (and deep-copy) constructor.
  MLKVUpsertContext(const MLKVUpsertContext& other)
    : key_{ other.key_ }
    , input_{ other.input_ }
    , length_{ other.length_ }
    , staleness_incr_{ other.staleness_incr_ }
    , staleness_bound_{ other.staleness_bound_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  const key_t& key() const {
    return key_;
  }
  inline int32_t value_size() const {
    return sizeof(value_t) + length_;
  }
  inline uint32_t value_size(const value_t& old_value) const {
    return sizeof(value_t) + length_;
  }

  /// Initial, non-atomic, and atomic RMW methods.
  inline void RmwInitial(value_t& value) {
    // assert(false);
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    GenLock before, after;
    before = old_value.gen_lock_.load();
    after.staleness = before.staleness + staleness_incr_;

    value.gen_lock_.store(after);
    value.size_ = sizeof(value_t) + length_;
    value.length_ = length_;

    std::memcpy(value.buffer(), input_, length_);
  }
  inline bool RmwAtomic(value_t& value) {
    bool replaced;
    while(!value.gen_lock_.try_lock(replaced, staleness_incr_, staleness_bound_)
          && !replaced) {
      std::this_thread::yield();
    }
    if(replaced) {
      // Some other thread replaced this record.
      return false;
    }
    if(value.size_ < sizeof(value_t) + length_) {
      // Current value is too small for in-place update.
      value.gen_lock_.unlock(true);
      return false;
    }
    // In-place update overwrites length and buffer, but not size.
    value.length_ = length_;
    std::memcpy(value.buffer(), input_, length_);
    value.gen_lock_.unlock(false);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  key_t key_;
  uint8_t* input_;
  uint64_t length_;
  int32_t staleness_incr_;
  int32_t staleness_bound_;
};

class MLKVLookaheadContext : public IAsyncContext {
  public:
   typedef Key key_t;
   typedef Value value_t;

   MLKVLookaheadContext(uint64_t key, uint64_t length)
     : key_{ key }
     , length_{ length } {
   }

   /// Copy (and deep-copy) constructor.
   MLKVLookaheadContext(const MLKVLookaheadContext& other)
     : key_{ other.key_ }
     , length_{ other.length_ } {
   }

   /// The implicit and explicit interfaces require a key() accessor.
   inline const key_t& key() const {
     return key_;
   }
   inline uint32_t value_size() const {
     return sizeof(value_t) + length_;
   }
   inline uint32_t value_size(const value_t& old_value) const {
     return sizeof(value_t) + old_value.length_;
   }
   inline void RmwInitial(value_t& value) {
     assert(false);
   }
   inline void RmwCopy(const value_t& old_value, value_t& value) {
     GenLock before, after;
     before = old_value.gen_lock_.load();
     after.staleness = before.staleness;

     value.gen_lock_.store(after);
     value.size_ = sizeof(value_t) + length_;
     value.length_ = length_;

     std::memcpy(value.buffer(), old_value.buffer(), old_value.length_);
   }
   inline bool RmwAtomic(value_t& value) {
     return true;
   }

   protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
   Status DeepCopy_Internal(IAsyncContext*& context_copy) {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   private:
    key_t key_;
    uint64_t length_;
};

typedef FASTER::environment::QueueIoHandler handler_t;
typedef FASTER::device::FileSystemDisk<handler_t, 1073741824L> disk_t;
using store_t = FasterKv<Key, Value, disk_t>;
struct faster_t {
  store_t* store;
};

faster_t* faster_open(const uint64_t table_size, const uint64_t log_size, const char* storage) {
  faster_t* res = new faster_t();
  std::experimental::filesystem::create_directory(storage);
  res->store= new store_t { table_size, log_size, storage, 0.8 };
  return res;
}

uint8_t faster_upsert(faster_t* faster_t, const uint64_t key, uint8_t* value, uint64_t value_length) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<UpsertContext> context{ ctxt };
  };

  UpsertContext context { key, value, value_length };
  Status result = faster_t->store->Upsert(context, callback, 1);
  return static_cast<uint8_t>(result);
}

uint8_t faster_rmw(faster_t* faster_t, const uint64_t key, uint8_t* incr, const uint64_t value_length) {
  auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
    CallbackContext<RmwContext> context{ ctxt };
  };
  RmwContext context{ key, incr, value_length };
  Status result = faster_t->store->Rmw(context, callback, 1);
  return static_cast<uint8_t>(result);
}

uint8_t faster_read(faster_t* faster_t, const uint64_t key, uint8_t* output) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<ReadContext> context{ ctxt };
  };

  ReadContext context {key, output};
  Status result = faster_t->store->Read(context, callback, 1);
  return static_cast<uint8_t>(result);
}

uint8_t faster_delete(faster_t* faster_t, const uint64_t key) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<DeleteContext> context { ctxt };
    assert(result == Status::Ok || result == Status::NotFound);
  };

  DeleteContext context {key};
  Status result = faster_t->store->Delete(context, callback, 1);
  return static_cast<uint8_t>(result);
}

uint8_t mlkv_read(faster_t* faster_t, const uint64_t key, uint8_t* output, const uint64_t value_length) {
  auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
    CallbackContext<MLKVReadContext> context{ ctxt };
  };
  MLKVReadContext context{ key, output, value_length, 1, 128 };
  Status result = faster_t->store->Rmw(context, callback, 1);
  if (context.found == false) {
    return static_cast<uint8_t>(FASTER::core::Status::NotFound);
  }
  return static_cast<uint8_t>(result);
}

uint8_t mlkv_upsert(faster_t* faster_t, const uint64_t key, uint8_t* value, uint64_t value_length) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<MLKVUpsertContext> context{ ctxt };
  };

  MLKVUpsertContext context { key, value, value_length, -1, 128 };
  Status result = faster_t->store->Rmw(context, callback, 1);
  return static_cast<uint8_t>(result);
}

uint8_t mlkv_lookahead(faster_t* faster_t, const uint64_t key, const uint64_t value_length) {
  auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
    CallbackContext<MLKVLookaheadContext> context{ ctxt };
  };
  MLKVLookaheadContext context{ key, value_length };
  Status result = faster_t->store->Rmw(context, callback, 1);
  return static_cast<uint8_t>(result);
}

faster_t* faster_recover(const uint64_t table_size, const uint64_t log_size, const char* storage, const char* checkpoint_token) {
  Guid token = Guid::Parse(checkpoint_token);
  faster_t* res = new faster_t();
  res->store= new store_t { table_size, log_size, storage, 0.8 };

  uint32_t version;
  std::vector<Guid> recovered_session_ids;
  Status status = res->store->Recover(token, token, version, recovered_session_ids);
  if(status != Status::Ok) {
    return nullptr;
  }

  std::vector<uint64_t> serial_nums;
  for(const auto& recovered_session_id : recovered_session_ids) {
    serial_nums.push_back(res->store->ContinueSession(recovered_session_id));
    res->store->StopSession();
  }
  return res;
}

bool faster_checkpoint(faster_t *faster_t) {
  static Guid token;
  static std::atomic<bool> index_checkpoint_completed;
  index_checkpoint_completed = false;
  static std::atomic<bool> hybrid_log_checkpoint_completed;
  hybrid_log_checkpoint_completed = false;

  auto index_persistence_callback = [](Status result) {
    index_checkpoint_completed = true;
  };
  auto hybrid_log_persistence_callback = [](Status result, uint64_t persistent_serial_num) {
    hybrid_log_checkpoint_completed = true;
  };

  faster_t->store->StartSession();
  bool result = faster_t->store->Checkpoint(index_persistence_callback, hybrid_log_persistence_callback, token);

  while(!index_checkpoint_completed) {
    faster_t->store->CompletePending(false);
  }
  while(!hybrid_log_checkpoint_completed) {
    faster_t->store->CompletePending(false);
  }
  faster_t->store->CompletePending(true);
  faster_t->store->StopSession();

  return result;
}

void faster_destroy(faster_t *faster_t) {
  if (faster_t == NULL)
    return;

  delete faster_t->store;
  delete faster_t;
}

// Thread-related
void faster_complete_pending(faster_t* faster_t, bool wait) {
  if (faster_t != NULL) {
    faster_t->store->CompletePending(wait);
  }
}

void faster_start_session(faster_t* faster_t) {
  if (faster_t != NULL) {
    faster_t->store->StartSession();
  }
}

void faster_stop_session(faster_t* faster_t) {
  if (faster_t != NULL) {
    faster_t->store->StopSession();
  }
}
} // extern "C"
