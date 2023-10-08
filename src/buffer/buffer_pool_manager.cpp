//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex>lock(latch_);
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    replacer_->RecordAccess(frame_id);
    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;
    pages_[*page_id].page_id_ = *page_id;
    ResetPage(&pages_[frame_id], *page_id);
    pages_[frame_id].pin_count_ = 1;
    return &pages_[frame_id];
  }
  frame_id_t replaced_frame = 0;
  if (replacer_->Evict(&replaced_frame)) {
    Page &page = pages_[replaced_frame];
    if (page.IsDirty()) {
      WritePageToDisk(page);
    }
    *page_id = AllocatePage();
    
    // delete information of old page
    page_table_.erase(page.page_id_);
    ResetPage(&pages_[replaced_frame], *page_id);
   
    // record information of new page
    page_table_[*page_id] = replaced_frame;
    pages_[replaced_frame].pin_count_ = 1;
    replacer_->RecordAccess(replaced_frame);
    return &pages_[replaced_frame];
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex>lock(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    replacer_->RecordAccess(frame_id);
    ResetPage(&pages_[frame_id], page_id);
    pages_[frame_id].pin_count_ = 1;
    // record information of new page
    page_table_[page_id] = frame_id;
    ReadPageFromDisk(pages_[frame_id]);
    return &pages_[frame_id];
  }
  frame_id_t replaced_frame = 0;
  if (replacer_->Evict(&replaced_frame)) {
    Page &page = pages_[replaced_frame];
    if (page.IsDirty()) {
      WritePageToDisk(page);
    }
    // delete information of old page
    ResetPage(&pages_[replaced_frame], page_id);
    page_table_.erase(page.page_id_);
    // record information of new page
    page_table_[page_id] = replaced_frame;
    pages_[replaced_frame].pin_count_ = 1;
    replacer_->RecordAccess(replaced_frame);
    ReadPageFromDisk(pages_[replaced_frame]);
    return &pages_[replaced_frame];
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex>lock(latch_);
  
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    Page &page = pages_[frame_id];
    if (page.pin_count_ == 0) {
      return false;
    }
    if (is_dirty) {
      page.is_dirty_ = true;
    }
    page.pin_count_--;
    if (page.pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  } 
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex>lock(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    Page &page = pages_[frame_id];
    WritePageToDisk(page);
    page.is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex>lock(latch_);
  for (auto kv: page_table_) {
    FlushPage(kv.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex>lock(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    Page &page = pages_[frame_id];
    if (page.pin_count_ != 0) {
      return false;
    }
    if (page.is_dirty_) {
      WritePageToDisk(page);
    }
    page_table_.erase(page_id);
    replacer_->Remove(frame_id);
    ResetPage(&page, INVALID_PAGE_ID);
    free_list_.push_back(frame_id);
    return true;
  }
  return false; 
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

void BufferPoolManager::ResetPage(Page *page, page_id_t page_id) {
  page->ResetMemory();
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
}

bool BufferPoolManager::WritePageToDisk(Page &page) {
  std::promise<bool> written;
  auto success = written.get_future();
  DiskRequest r({true, page.data_, page.page_id_, std::move(written)});
  disk_scheduler_->Schedule(std::move(r));
  return success.get();
}

bool BufferPoolManager::ReadPageFromDisk(Page &page) {
  std::promise<bool> read;
  auto success = read.get_future();
  DiskRequest r({false, page.data_, page.page_id_, std::move(read)});
  disk_scheduler_->Schedule(std::move(r));
  return success.get();
}



}  // namespace bustub
