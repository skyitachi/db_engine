#ifndef ENGINE_RACE_SKIPLIST_H_
#define ENGINE_RACE_SKIPLIST_H_
#include <assert.h>
#include <stdlib.h>
#include <ctime>
#include <iostream>
#include <vector>

namespace polar_race {

template<typename Key, class Comparator>
class SkipList {
  public:
    struct Node;
  
  public:
    explicit SkipList(Comparator cmp);
    void Insert(const Key& key);
    bool Contains(const Key& key);
    Node* Head() const { return head_; }
    
    class Iterator {
      public:
        // Initialize an iterator over the specified list.
        // The returned iterator is not valid.
        explicit Iterator(const SkipList* list);

        // Returns true iff the iterator is positioned at a valid node.
        bool Valid() const;

        // Returns the key at the current position.
        // REQUIRES: Valid()
        const Key& key() const;

        // Advances to the next position.
        // REQUIRES: Valid()
        void Next();

        // Advances to the previous position.
        // REQUIRES: Valid()
        void Prev();

        // Advance to the first entry with a key >= target
        void Seek(const Key& target);

        // Position at the first entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        void SeekToFirst();

        // Position at the last entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        void SeekToLast();

      private:
        const SkipList* list_;
        Node* node_;
        // Intentionally copyable
    };
  // private: 
  public:
    enum { kMaxHeight = 12};
    Comparator const compare_;
    Node* const head_;
    // max_height
    inline int GetMaxHeight() const {
      return max_height_; 
    }
    Node* NewNode(const Key& key, int height);
    int RandomHeight();
    bool Equal(const Key& a, const Key &b) const { return compare_(a, b) == 0; }
    
    bool KeyIsAfterNode(const Key& key, Node* n) const;
    
    Node* FindGreaterOrEqual(const Key& key, Node **prev) const;

    Node* FindLessThan(const Key& key) const;
    
    Node* FindLast() const;

    SkipList(const SkipList&);
    void operator=(const SkipList&);
    int max_height_;
};

template<typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp)
    : compare_(cmp), head_(NewNode(0, kMaxHeight)), max_height_(1) {
  std::srand(std::time(nullptr));
}

template<typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k): key(k) {
    next_.resize(kMaxHeight);
  }

  Key const key;

  Node* Next(int n) {
    assert(n >= 0 && n < kMaxHeight);
    assert(!next_.empty());
    Node* ret = next_[n];
    return next_[n];
  }
  
  void SetNext(int n, Node* x) {
    assert(n >= 0 && n < kMaxHeight);
    next_[n] = x;
  }
private:
  std::vector<Node*> next_;
};

// TODO: read it later
template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::NewNode(const Key& key, int height) {
  return new Node(key);
}

template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

// 如何更新 prev
template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node **prev) 
    const {
  Node* it = head_;
  int level = GetMaxHeight() - 1;
  while(true) {
    Node* next = it->Next(level);
    if (KeyIsAfterNode(key, next)) {
      it = next;
    } else {
      if (prev != nullptr) { 
        prev[level] = it;
      };
      if (level == 0) {
        return next;
      } else {
        level--;
      }
    } 
  }
}

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLessThan(const Key& key) const {

}

template<typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast() const {
  
}

template<typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && (std::rand() % kBranching == 0)) {
    height++;
  }
  return height;
}

template<typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  Node* prev[kMaxHeight];
  // Note: LastNext的作用
  Node *lastNext = FindGreaterOrEqual(key, prev);
  int max_height = GetMaxHeight();
  int height = RandomHeight();
  // printf("key is %llu, random height is %d\n", key, height);
  if (max_height < height) {
    for (int i = max_height; i < height; i++) {
      prev[i] = head_; 
    }
    max_height_ = height;
  }
  Node* node = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    assert(prev[i] != nullptr);
    node->SetNext(i, prev[i]->Next(i));
    prev[i]->SetNext(i, node);    
  }
}

template<typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) {
  Node* it = FindGreaterOrEqual(key, nullptr);
  if (it != nullptr && !compare_(key, it->key)) {
    return true;
  }
  return false;
}


template<typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template<typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template<typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {

}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  
}

template<typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}


}

#endif