//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  for (auto it = node_list_.begin(); it != node_list_.end(); it++) {
    if (it->IsEvictable()) {
      *frame_id = it->GetFrameId();
      node_list_.erase(it);
      node_store_.erase(node_store_.find(*frame_id));
      latch_.unlock();
      curr_size_--;
      return true;
    }
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  latch_.lock();
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    LRUKNode new_node = LRUKNode(frame_id, k_);
    new_node.PushHistory(current_timestamp_);
    auto new_it = Insert(node_list_.begin(), new_node);
    node_store_[frame_id] = new_it;
  } else {
    LRUKNode node;
    LRUKNode new_node(std::move(*(it->second)));
    new_node.PushHistory(current_timestamp_);
    auto new_it = Insert(it->second, new_node);
    node_list_.erase(it->second);
    node_store_[frame_id] = new_it;
  }
  current_timestamp_++;
  if (node_list_.size() > replacer_size_) {
    int a;
    Evict(&a);
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    auto before = it->second->IsEvictable();
    it->second->SetEvictable(set_evictable);
    if (!before && set_evictable) {
      curr_size_++;
    }
    if (before && !set_evictable) {
      curr_size_--;
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    BUSTUB_ASSERT(it->second->IsEvictable(), "ERROR: remove a inevictable frame.");
    curr_size_--;
    node_list_.erase(it->second);
    node_store_.erase(it);
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::Insert(std::list<LRUKNode>::iterator start, const LRUKNode &node) -> std::list<LRUKNode>::iterator {
  while (start != node_list_.end() && *start < node) {
    start++;
  }
  return node_list_.insert(start, node);
}

}  // namespace bustub
