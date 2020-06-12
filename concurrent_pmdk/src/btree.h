/*
   Copyright (c) 2018, UNIST. All rights reserved. The license is a free
   non-exclusive, non-transferable license to reproduce, use, modify and display
   the source code version of the Software, with or without modifications solely
   for non-commercial research, educational or evaluation purposes. The license
   does not entitle Licensee to technical support, telephone assistance,
   enhancements or updates to the Software. All rights, title to and ownership
   interest in the Software, including all intellectual property rights therein
   shall remain in UNIST.

   Please use at your own risk.
*/

#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#include <libpmemobj.h>
#include <math.h>
#include <shared_mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <vector>

#define PAGESIZE (512)

#define CACHE_LINE_SIZE 64

#define IS_FORWARD(c) (c % 2 == 0)

class btree;
class page;

POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_ROOT(btree, btree);
POBJ_LAYOUT_TOID(btree, page);
POBJ_LAYOUT_END(btree);

using entry_key_t = int64_t;

pthread_mutex_t print_mtx;

using namespace std;

class btree {
private:
  int height;
  TOID(page) root;
  PMEMobjpool *pop;

public:
  btree();
  void constructor(PMEMobjpool *);
  void setNewRoot(TOID(page));
  void btree_insert(entry_key_t, char *);
  void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
  void btree_delete(entry_key_t);
  void btree_delete_internal(entry_key_t, char *, uint32_t, entry_key_t *,
                             bool *, page **);
  char *btree_search(entry_key_t);
  void btree_search_range(entry_key_t, entry_key_t, unsigned long *);
  void printAll();
  void randScounter();

  friend class page;
};

class header {
private:
  TOID(page) sibling_ptr;   // 16 bytes
  page *leftmost_ptr;       // 8 bytes
  uint32_t level;           // 4 bytes
  uint8_t switch_counter;   // 1 bytes
  uint8_t is_deleted;       // 1 bytes
  int16_t last_index;       // 2 bytes
  pthread_rwlock_t *rwlock; // 8 bytes
  char dummy[8];            // 8 bytes

  friend class page;
  friend class btree;

public:
  void constructor() {
    rwlock = new pthread_rwlock_t;
    if (pthread_rwlock_init(rwlock, NULL)) {
      perror("lock init fail");
      exit(1);
    }

    leftmost_ptr = NULL;
    TOID_ASSIGN(sibling_ptr, pmemobj_oid(this));
    sibling_ptr.oid.off = 0;
    switch_counter = 0;
    last_index = -1;
    is_deleted = false;
  }

  ~header() {
    pthread_rwlock_destroy(rwlock);
    delete rwlock;
  }
};

class entry {
private:
  entry_key_t key; // 8 bytes
  char *ptr;       // 8 bytes

public:
  void constructor() {
    key = LONG_MAX;
    ptr = NULL;
  }

  friend class page;
  friend class btree;
};

const int cardinality = (PAGESIZE - sizeof(header)) / sizeof(entry);
const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

class page {
private:
  header hdr;                 // header in persistent memory, 48 bytes
  entry records[cardinality]; // slots in persistent memory, 16 bytes * n

public:
  friend class btree;

  void constructor(uint32_t level = 0) {
    hdr.constructor();
    for (int i = 0; i < cardinality; i++) {
      records[i].key = LONG_MAX;
      records[i].ptr = NULL;
    }

    hdr.level = level;
    records[0].ptr = NULL;
  }

  // this is called when tree grows
  void constructor(PMEMobjpool *pop, page *left, entry_key_t key, page *right,
                   uint32_t level = 0) {
    hdr.constructor();
    for (int i = 0; i < cardinality; i++) {
      records[i].key = LONG_MAX;
      records[i].ptr = NULL;
    }

    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (char *)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;

    pmemobj_persist(pop, this, sizeof(page));
  }

  inline int count() {
    uint8_t previous_switch_counter;
    int count = 0;

    do {
      previous_switch_counter = hdr.switch_counter;
      count = hdr.last_index + 1;

      while (count >= 0 && records[count].ptr != NULL) {
        if (IS_FORWARD(previous_switch_counter))
          ++count;
        else
          --count;
      }

      if (count < 0) {
        count = 0;
        while (records[count].ptr != NULL) {
          ++count;
        }
      }

    } while (previous_switch_counter != hdr.switch_counter);

    return count;
  }

  inline bool remove_key(PMEMobjpool *pop, entry_key_t key) {
    // Set the switch_counter
    if (IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    bool shift = false;
    int i;
    for (i = 0; records[i].ptr != NULL; ++i) {
      if (!shift && records[i].key == key) {
        records[i].ptr =
            (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
        shift = true;
      }

      if (shift) {
        records[i].key = records[i + 1].key;
        records[i].ptr = records[i + 1].ptr;

        // flush
        uint64_t records_ptr = (uint64_t)(&records[i]);
        int remainder = records_ptr % CACHE_LINE_SIZE;
        bool do_flush =
            (remainder == 0) ||
            ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) &&
             ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
        if (do_flush) {
          pmemobj_persist(pop, (void *)records_ptr, CACHE_LINE_SIZE);
        }
      }
    }

    if (shift) {
      --hdr.last_index;
    }
    return shift;
  }

  bool remove(btree *bt, entry_key_t key, bool only_rebalance = false,
              bool with_lock = true) {
    pthread_rwlock_wrlock(hdr.rwlock);

    bool ret = remove_key(bt->pop, key);

    pthread_rwlock_unlock(hdr.rwlock);

    return ret;
  }

  /*
   * Although we implemented the rebalancing of B+-Tree, it is currently blocked
   * for the performance. Please refer to the follow. Chi, P., Lee, W. C., &
   * Xie, Y. (2014, August). Making B+-tree efficient in PCM-based main memory.
   * In Proceedings of the 2014 international symposium on Low power electronics
   * and design (pp. 69-74). ACM.
   */

  bool remove_rebalancing(btree *bt, entry_key_t key,
                          bool only_rebalance = false, bool with_lock = true) {
    if (with_lock) {
      pthread_rwlock_wrlock(hdr.rwlock);
    }
    if (hdr.is_deleted) {
      if (with_lock) {
        pthread_rwlock_unlock(hdr.rwlock);
      }
      return false;
    }

    if (!only_rebalance) {
      register int num_entries_before = count();

      // This node is root
      if (this == D_RO(bt->root)) {
        if (hdr.level > 0) {
          if (num_entries_before == 1 && (hdr.sibling_ptr.oid.off == 0)) {
            bt->root.oid.off = (uint64_t)hdr.leftmost_ptr;
            pmemobj_persist(bt->pop, &(bt->root), sizeof(TOID(page)));

            hdr.is_deleted = 1;
          }
        }

        // Remove the key from this node
        bool ret = remove_key(bt->pop, key);

        if (with_lock) {
          pthread_rwlock_unlock(hdr.rwlock);
        }
        return true;
      }

      bool should_rebalance = true;
      // check the node utilization
      if (num_entries_before - 1 >= (int)((cardinality - 1) * 0.5)) {
        should_rebalance = false;
      }

      // Remove the key from this node
      bool ret = remove_key(bt->pop, key);

      if (!should_rebalance) {
        if (with_lock) {
          pthread_rwlock_unlock(hdr.rwlock);
        }
        return (hdr.leftmost_ptr == NULL) ? ret : true;
      }
    }

    // Remove a key from the parent node
    entry_key_t deleted_key_from_parent = 0;
    bool is_leftmost_node = false;
    TOID(page) left_sibling;
    left_sibling.oid.pool_uuid_lo = bt->root.oid.pool_uuid_lo;
    bt->btree_delete_internal(key, (char *)pmemobj_oid(this).off, hdr.level + 1,
                              &deleted_key_from_parent, &is_leftmost_node,
                              (page **)&left_sibling.oid.off);

    if (is_leftmost_node) {
      if (with_lock) {
        pthread_rwlock_unlock(hdr.rwlock);
      }

      if (!with_lock) {
        pthread_rwlock_wrlock(D_RW(hdr.sibling_ptr)->hdr.rwlock);
      }

      D_RW(hdr.sibling_ptr)
          ->remove(bt, D_RW(hdr.sibling_ptr)->records[0].key, true, with_lock);

      if (!with_lock) {
        pthread_rwlock_unlock(D_RW(hdr.sibling_ptr)->hdr.rwlock);
      }
      return true;
    }

    if (with_lock) {
      pthread_rwlock_wrlock(D_RW(left_sibling)->hdr.rwlock);
    }

    while (D_RO(left_sibling)->hdr.sibling_ptr.oid.off !=
           pmemobj_oid(this).off) {
      if (with_lock) {
        uint64_t t = D_RO(left_sibling)->hdr.sibling_ptr.oid.off;
        pthread_rwlock_unlock(D_RW(left_sibling)->hdr.rwlock);
        left_sibling.oid.off = t;
        pthread_rwlock_wrlock(D_RW(left_sibling)->hdr.rwlock);
      } else
        left_sibling = D_RO(left_sibling)->hdr.sibling_ptr;
    }

    register int num_entries = count();
    register int left_num_entries = D_RW(left_sibling)->count();

    // Merge or Redistribution
    int total_num_entries = num_entries + left_num_entries;
    if (hdr.leftmost_ptr)
      ++total_num_entries;

    entry_key_t parent_key;

    if (total_num_entries > cardinality - 1) { // Redistribution
      register int m = (int)ceil(total_num_entries / 2);

      if (num_entries < left_num_entries) { // left -> right
        if (hdr.leftmost_ptr == nullptr) {
          for (int i = left_num_entries - 1; i >= m; i--) {
            insert_key(bt->pop, D_RW(left_sibling)->records[i].key,
                       D_RW(left_sibling)->records[i].ptr, &num_entries);
          }

          D_RW(left_sibling)->records[m].ptr = nullptr;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->records[m].ptr),
                          sizeof(char *));

          D_RW(left_sibling)->hdr.last_index = m - 1;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.last_index),
                          sizeof(int16_t));

          parent_key = records[0].key;
        } else {
          insert_key(bt->pop, deleted_key_from_parent, (char *)hdr.leftmost_ptr,
                     &num_entries);

          for (int i = left_num_entries - 1; i > m; i--) {
            insert_key(bt->pop, D_RO(left_sibling)->records[i].key,
                       D_RO(left_sibling)->records[i].ptr, &num_entries);
          }

          parent_key = D_RO(left_sibling)->records[m].key;

          hdr.leftmost_ptr = (page *)D_RO(left_sibling)->records[m].ptr;
          pmemobj_persist(bt->pop, &(hdr.leftmost_ptr), sizeof(page *));

          D_RW(left_sibling)->records[m].ptr = nullptr;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->records[m].ptr),
                          sizeof(char *));

          D_RW(left_sibling)->hdr.last_index = m - 1;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.last_index),
                          sizeof(int16_t));
        }

        if (left_sibling.oid.off == bt->root.oid.off) {
          TOID(page) new_root;
          POBJ_NEW(bt->pop, &new_root, page, NULL, NULL);
          D_RW(new_root)->constructor(bt->pop, (page *)left_sibling.oid.off,
                                      parent_key, (page *)pmemobj_oid(this).off,
                                      hdr.level + 1);
          bt->setNewRoot(new_root);
        } else {
          bt->btree_insert_internal((char *)left_sibling.oid.off, parent_key,
                                    (char *)pmemobj_oid(this).off,
                                    hdr.level + 1);
        }
      } else { // from leftmost case
        hdr.is_deleted = 1;
        pmemobj_persist(bt->pop, &(hdr.is_deleted), sizeof(uint8_t));

        TOID(page) new_sibling;
        POBJ_NEW(bt->pop, &new_sibling, page, NULL, NULL);
        D_RW(new_sibling)->constructor(hdr.level);
        pthread_rwlock_wrlock(D_RW(new_sibling)->hdr.rwlock);
        D_RW(new_sibling)->hdr.sibling_ptr = hdr.sibling_ptr;

        int num_dist_entries = num_entries - m;
        int new_sibling_cnt = 0;

        if (hdr.leftmost_ptr == nullptr) {
          for (int i = 0; i < num_dist_entries; i++) {
            D_RW(left_sibling)
                ->insert_key(bt->pop, records[i].key, records[i].ptr,
                             &left_num_entries);
          }

          for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
            D_RW(new_sibling)
                ->insert_key(bt->pop, records[i].key, records[i].ptr,
                             &new_sibling_cnt, false);
          }

          pmemobj_persist(bt->pop, D_RW(new_sibling), sizeof(page));

          D_RW(left_sibling)->hdr.sibling_ptr = new_sibling;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.sibling_ptr),
                          sizeof(page *));

          parent_key = D_RO(new_sibling)->records[0].key;
        } else {
          D_RW(left_sibling)
              ->insert_key(bt->pop, deleted_key_from_parent,
                           (char *)hdr.leftmost_ptr, &left_num_entries);

          for (int i = 0; i < num_dist_entries - 1; i++) {
            D_RW(left_sibling)
                ->insert_key(bt->pop, records[i].key, records[i].ptr,
                             &left_num_entries);
          }

          parent_key = records[num_dist_entries - 1].key;

          D_RW(new_sibling)->hdr.leftmost_ptr =
              (page *)records[num_dist_entries - 1].ptr;
          for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
            D_RW(new_sibling)
                ->insert_key(bt->pop, records[i].key, records[i].ptr,
                             &new_sibling_cnt, false);
          }
          pmemobj_persist(bt->pop, D_RW(new_sibling), sizeof(page));

          D_RW(left_sibling)->hdr.sibling_ptr = new_sibling;
          pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.sibling_ptr),
                          sizeof(page *));
        }

        if (left_sibling.oid.off == bt->root.oid.off) {
          TOID(page) new_root;
          POBJ_NEW(bt->pop, &new_root, page, NULL, NULL);
          D_RW(new_root)->constructor(bt->pop, (page *)left_sibling.oid.off,
                                      parent_key, (page *)new_sibling.oid.off,
                                      hdr.level + 1);
          bt->setNewRoot(new_root);
        } else {
          bt->btree_insert_internal((char *)left_sibling.oid.off, parent_key,
                                    (char *)new_sibling.oid.off, hdr.level + 1);
        }

        pthread_rwlock_unlock(D_RW(new_sibling)->hdr.rwlock);
      }
    } else {
      hdr.is_deleted = 1;
      pmemobj_persist(bt->pop, &(hdr.is_deleted), sizeof(uint8_t));

      if (hdr.leftmost_ptr)
        D_RW(left_sibling)
            ->insert_key(bt->pop, deleted_key_from_parent,
                         (char *)hdr.leftmost_ptr, &left_num_entries);

      for (int i = 0; records[i].ptr != NULL; ++i) {
        D_RW(left_sibling)
            ->insert_key(bt->pop, records[i].key, records[i].ptr,
                         &left_num_entries);
      }

      D_RW(left_sibling)->hdr.sibling_ptr = hdr.sibling_ptr;
      pmemobj_persist(bt->pop, &(D_RW(left_sibling)->hdr.sibling_ptr),
                      sizeof(page *));
    }

    if (with_lock) {
      pthread_rwlock_unlock(D_RW(left_sibling)->hdr.rwlock);
      pthread_rwlock_unlock(hdr.rwlock);
    }

    return true;
  }

  inline void insert_key(PMEMobjpool *pop, entry_key_t key, char *ptr,
                         int *num_entries, bool flush = true,
                         bool update_last_index = true) {
    // update switch_counter
    if (!IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    // FAST
    if (*num_entries == 0) { // this page is empty
      entry *new_entry = (entry *)&records[0];
      entry *array_end = (entry *)&records[1];
      new_entry->key = (entry_key_t)key;
      new_entry->ptr = (char *)ptr;

      array_end->ptr = (char *)NULL;

      if (flush) {
        pmemobj_persist(pop, this, CACHE_LINE_SIZE);
      }
    } else {
      int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
      records[*num_entries + 1].ptr = records[*num_entries].ptr;

      if (flush) {
        if ((uint64_t) & (records[*num_entries + 1]) % CACHE_LINE_SIZE == 0)
          pmemobj_persist(pop, &records[*num_entries + 1].ptr, sizeof(char *));
      }

      // FAST
      for (i = *num_entries - 1; i >= 0; i--) {
        if (key < records[i].key) {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = records[i].key;

          if (flush) {
            uint64_t records_ptr = (uint64_t)(&records[i + 1]);

            int remainder = records_ptr % CACHE_LINE_SIZE;
            bool do_flush =
                (remainder == 0) ||
                ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) &&
                 ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
            if (do_flush) {
              pmemobj_persist(pop, (void *)records_ptr, CACHE_LINE_SIZE);
              to_flush_cnt = 0;
            } else
              ++to_flush_cnt;
          }
        } else {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = key;
          records[i + 1].ptr = ptr;

          if (flush)
            pmemobj_persist(pop, &records[i + 1], sizeof(entry));
          inserted = 1;
          break;
        }
      }
      if (inserted == 0) {
        records[0].ptr = (char *)hdr.leftmost_ptr;
        records[0].key = key;
        records[0].ptr = ptr;

        if (flush)
          pmemobj_persist(pop, &records[0], sizeof(entry));
      }
    }

    if (update_last_index) {
      hdr.last_index = *num_entries;
    }
    ++(*num_entries);
  }

  // Insert a new key - FAST and FAIR
  page *store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
              bool with_lock, page *invalid_sibling = NULL) {
    if (with_lock) {
      pthread_rwlock_wrlock(hdr.rwlock);
    }
    if (hdr.is_deleted) {
      if (with_lock) {
        pthread_rwlock_unlock(hdr.rwlock);
      }

      return NULL;
    }

    // If this node has a sibling node,
    if ((hdr.sibling_ptr.oid.off != 0) &&
        ((page *)hdr.sibling_ptr.oid.off != invalid_sibling)) {
      // Compare this key with the first key of the sibling
      if (key > D_RO(hdr.sibling_ptr)->records[0].key) {
        if (with_lock) {
          pthread_rwlock_unlock(hdr.rwlock);
        }

        return D_RW(hdr.sibling_ptr)
            ->store(bt, NULL, key, right, true, with_lock, invalid_sibling);
      }
    }

    register int num_entries = count();

    // FAST
    if (num_entries < cardinality - 1) {
      insert_key(bt->pop, key, right, &num_entries, flush);

      if (with_lock) {
        pthread_rwlock_unlock(hdr.rwlock);
      }

      return (page *)pmemobj_oid(this).off;
    } else { // FAIR
      // overflow
      // create a new node
      TOID(page) sibling;
      POBJ_NEW(bt->pop, &sibling, page, NULL, NULL);
      D_RW(sibling)->constructor(hdr.level);
      page *sibling_ptr = D_RW(sibling);
      register int m = (int)ceil(num_entries / 2);
      entry_key_t split_key = records[m].key;

      // migrate half of keys into the sibling
      int sibling_cnt = 0;
      if (hdr.leftmost_ptr == NULL) { // leaf node
        for (int i = m; i < num_entries; ++i) {
          sibling_ptr->insert_key(bt->pop, records[i].key, records[i].ptr,
                                  &sibling_cnt, false);
        }
      } else { // internal node
        for (int i = m + 1; i < num_entries; ++i) {
          sibling_ptr->insert_key(bt->pop, records[i].key, records[i].ptr,
                                  &sibling_cnt, false);
        }
        sibling_ptr->hdr.leftmost_ptr = (page *)records[m].ptr;
      }

      sibling_ptr->hdr.sibling_ptr = hdr.sibling_ptr;
      pmemobj_persist(bt->pop, sibling_ptr, sizeof(page));

      hdr.sibling_ptr = sibling;
      pmemobj_persist(bt->pop, &hdr, sizeof(hdr));

      // set to NULL
      if (IS_FORWARD(hdr.switch_counter))
        hdr.switch_counter += 2;
      else
        ++hdr.switch_counter;
      records[m].ptr = NULL;
      pmemobj_persist(bt->pop, &records[m], sizeof(entry));

      hdr.last_index = m - 1;
      pmemobj_persist(bt->pop, &hdr.last_index, sizeof(int16_t));

      num_entries = hdr.last_index + 1;

      page *ret;

      // insert the key
      if (key < split_key) {
        insert_key(bt->pop, key, right, &num_entries);
        ret = (page *)pmemobj_oid(this).off;
      } else {
        sibling_ptr->insert_key(bt->pop, key, right, &sibling_cnt);
        ret = (page *)sibling.oid.off;
      }

      // Set a new root or insert the split key to the parent
      if (D_RO(bt->root) == this) { // only one node can update the root ptr
        TOID(page) new_root;
        POBJ_NEW(bt->pop, &new_root, page, NULL, NULL);
        D_RW(new_root)->constructor(bt->pop, (page *)bt->root.oid.off,
                                    split_key, (page *)sibling.oid.off,
                                    hdr.level + 1);
        bt->setNewRoot(new_root);

        if (with_lock) {
          pthread_rwlock_unlock(hdr.rwlock); // Unlock the write lock
        }
      } else {
        if (with_lock) {
          pthread_rwlock_unlock(hdr.rwlock); // Unlock the write lock
        }
        bt->btree_insert_internal(NULL, split_key, (char *)sibling.oid.off,
                                  hdr.level + 1);
      }

      return ret;
    }
  }

  // Search keys with linear search
  void linear_search_range(entry_key_t min, entry_key_t max,
                           unsigned long *buf) {
    int i, off = 0;
    uint8_t previous_switch_counter;
    page *current = this;

    while (current) {
      pthread_rwlock_rdlock(current->hdr.rwlock);
      int old_off = off;
      do {
        previous_switch_counter = current->hdr.switch_counter;
        off = old_off;

        entry_key_t tmp_key;
        char *tmp_ptr;

        if (IS_FORWARD(previous_switch_counter)) {
          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else {
              pthread_rwlock_unlock(current->hdr.rwlock);
              return;
            }
          }

          for (i = 1; current->records[i].ptr != NULL; ++i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else {
                pthread_rwlock_unlock(current->hdr.rwlock);
                return;
              }
            }
          }
        } else {
          for (i = current->count() - 1; i > 0; --i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else {
                pthread_rwlock_unlock(current->hdr.rwlock);
                return;
              }
            }
          }

          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else {
              pthread_rwlock_unlock(current->hdr.rwlock);
              return;
            }
          }
        }
      } while (previous_switch_counter != current->hdr.switch_counter);

      pthread_rwlock_unlock(current->hdr.rwlock);
      current = D_RW(current->hdr.sibling_ptr);
    }
  }

  char *linear_search(entry_key_t key) {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;
    entry_key_t k;

    if (hdr.leftmost_ptr == NULL) {      // Search a leaf node
      pthread_rwlock_rdlock(hdr.rwlock); // Lock Read Lock
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        // search from left ro right
        if (IS_FORWARD(previous_switch_counter)) {
          if ((k = records[0].key) == key) {
            if ((t = records[0].ptr) != NULL) {
              if (k == records[0].key) {
                ret = t;
                continue;
              }
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr)) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }
        } else { // search from right to left
          for (i = count() - 1; i > 0; --i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr) && t) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }

          if (!ret) {
            if ((k = records[0].key) == key) {
              if (NULL != (t = records[0].ptr) && t) {
                if (k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if (ret) {
        pthread_rwlock_unlock(hdr.rwlock);
        return ret;
      }

      if ((t = (char *)hdr.sibling_ptr.oid.off) &&
          key >= D_RW(hdr.sibling_ptr)->records[0].key) {
        pthread_rwlock_unlock(hdr.rwlock);
        return t;
      }

      pthread_rwlock_unlock(hdr.rwlock);
      return NULL;
    } else { // internal node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        if (IS_FORWARD(previous_switch_counter)) {
          if (key < (k = records[0].key)) {
            if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
              ret = t;
              continue;
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if (key < (k = records[i].key)) {
              if ((t = records[i - 1].ptr) != records[i].ptr) {
                ret = t;
                break;
              }
            }
          }

          if (!ret) {
            ret = records[i - 1].ptr;
            continue;
          }
        } else { // search from right to left
          for (i = count() - 1; i >= 0; --i) {
            if (key >= (k = records[i].key)) {
              if (i == 0) {
                if ((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              } else {
                if (records[i - 1].ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if ((t = (char *)hdr.sibling_ptr.oid.off) != NULL) {
        if (key >= D_RW(hdr.sibling_ptr)->records[0].key)
          return t;
      }

      if (ret) {
        return ret;
      } else
        return (char *)hdr.leftmost_ptr;
    }

    return NULL;
  }

  // print a node
  void print() {
    if (hdr.leftmost_ptr == NULL)
      printf("[%d] leaf %x \n", this->hdr.level, pmemobj_oid(this).off);
    else
      printf("[%d] internal %x \n", this->hdr.level, pmemobj_oid(this).off);
    printf("last_index: %d\n", hdr.last_index);
    printf("switch_counter: %d\n", hdr.switch_counter);
    printf("search direction: ");
    if (IS_FORWARD(hdr.switch_counter))
      printf("->\n");
    else
      printf("<-\n");

    if (hdr.leftmost_ptr != NULL)
      printf("%x ", hdr.leftmost_ptr);

    for (int i = 0; records[i].ptr != NULL; ++i)
      printf("%ld,%x ", records[i].key, records[i].ptr);

    printf("%x ", hdr.sibling_ptr.oid.off);

    printf("\n");
  }

  void printAll() {
    TOID(page) p = TOID_NULL(page);
    TOID_ASSIGN(p, pmemobj_oid(this));

    if (hdr.leftmost_ptr == NULL) {
      printf("printing leaf node: ");
      print();
    } else {
      printf("printing internal node: ");
      print();
      p.oid.off = (uint64_t)hdr.leftmost_ptr;
      D_RW(p)->printAll();
      for (int i = 0; records[i].ptr != NULL; ++i) {
        p.oid.off = (uint64_t)records[i].ptr;
        D_RW(p)->printAll();
      }
    }
  }
};

/*
 * class btree
 */
void btree::constructor(PMEMobjpool *pool) {
  pop = pool;
  POBJ_NEW(pop, &root, page, NULL, NULL);
  D_RW(root)->constructor();
  height = 1;
}

void btree::setNewRoot(TOID(page) new_root) {
  root = new_root;
  pmemobj_persist(pop, &root, sizeof(TOID(page)));
  ++height;
}

char *btree::btree_search(entry_key_t key) {
  TOID(page) p = root;

  while (D_RO(p)->hdr.leftmost_ptr != NULL) {
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
  }

  uint64_t t;
  while ((t = (uint64_t)D_RW(p)->linear_search(key)) ==
         D_RO(p)->hdr.sibling_ptr.oid.off) {
    p.oid.off = t;
    if (!t) {
      break;
    }
  }

  if (!t) {
    printf("NOT FOUND %lu, t = %x\n", key, t);
    return NULL;
  }

  return (char *)t;
}

// insert the key in the leaf node
void btree::btree_insert(entry_key_t key, char *right) {
  TOID(page) p = root;

  while (D_RO(p)->hdr.leftmost_ptr != NULL) {
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
  }

  if (!D_RW(p)->store(this, NULL, key, right, true, true)) { // store
    btree_insert(key, right);
  }
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right,
                                  uint32_t level) {
  if (level > D_RO(root)->hdr.level)
    return;

  TOID(page) p = root;

  while (D_RO(p)->hdr.level > level)
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);

  if (!D_RW(p)->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btree_delete(entry_key_t key) {
  TOID(page) p = root;

  while (D_RO(p)->hdr.leftmost_ptr != NULL) {
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
  }

  uint64_t t;
  while ((t = (uint64_t)(D_RW(p)->linear_search(key))) ==
         D_RW(p)->hdr.sibling_ptr.oid.off) {
    p.oid.off = t;
    if (!t)
      break;
  }

  if (t) {
    if (!D_RW(p)->remove(this, key)) {
      btree_delete(key);
    }
  } else {
    printf("not found the key to delete %lu\n", key);
  }
}

void btree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level,
                                  entry_key_t *deleted_key,
                                  bool *is_leftmost_node, page **left_sibling) {
  if (level > D_RO(root)->hdr.level)
    return;

  TOID(page) p = root;

  while (D_RW(p)->hdr.level > level) {
    p.oid.off = (uint64_t)D_RW(p)->linear_search(key);
  }

  pthread_rwlock_wrlock(D_RW(p)->hdr.rwlock);

  if ((char *)D_RO(p)->hdr.leftmost_ptr == ptr) {
    *is_leftmost_node = true;
    pthread_rwlock_unlock(D_RW(p)->hdr.rwlock);
    return;
  }

  *is_leftmost_node = false;

  for (int i = 0; D_RO(p)->records[i].ptr != NULL; ++i) {
    if (D_RO(p)->records[i].ptr == ptr) {
      if (i == 0) {
        if ((char *)D_RO(p)->hdr.leftmost_ptr != D_RO(p)->records[i].ptr) {
          *deleted_key = D_RO(p)->records[i].key;
          *left_sibling = D_RO(p)->hdr.leftmost_ptr;
          D_RW(p)->remove(this, *deleted_key, false, false);
          break;
        }
      } else {
        if (D_RO(p)->records[i - 1].ptr != D_RO(p)->records[i].ptr) {
          *deleted_key = D_RO(p)->records[i].key;
          *left_sibling = (page *)D_RO(p)->records[i - 1].ptr;
          D_RW(p)->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }

  pthread_rwlock_unlock(D_RW(p)->hdr.rwlock);
}

// Function to search keys from "min" to "max"
void btree::btree_search_range(entry_key_t min, entry_key_t max,
                               unsigned long *buf) {
  TOID(page) p = root;

  while (p.oid.off != 0) {
    if (D_RO(p)->hdr.leftmost_ptr != NULL) {
      // The current page is internal
      p.oid.off = (uint64_t)D_RW(p)->linear_search(min);
    } else {
      // Found a leaf
      D_RW(p)->linear_search_range(min, max, buf);

      break;
    }
  }
}

void btree::printAll() {
  pthread_mutex_lock(&print_mtx);
  int total_keys = 0;
  TOID(page) leftmost = root;
  printf("root: %x\n", root.oid.off);
  if (root.oid.off) {
    do {
      TOID(page) sibling = leftmost;
      while (sibling.oid.off) {
        if (D_RO(sibling)->hdr.level == 0) {
          total_keys += D_RO(sibling)->hdr.last_index + 1;
        }
        D_RW(sibling)->print();
        sibling = D_RO(sibling)->hdr.sibling_ptr;
      }
      printf("-----------------------------------------\n");
      leftmost.oid.off = (uint64_t)D_RO(leftmost)->hdr.leftmost_ptr;
    } while (leftmost.oid.off != 0);
  }

  printf("total number of keys: %d\n", total_keys);
  pthread_mutex_unlock(&print_mtx);
}

void btree::randScounter() {
  TOID(page) leftmost = root;
  srand(time(NULL));
  if (root.oid.off) {
    do {
      TOID(page) sibling = leftmost;
      while (sibling.oid.off) {
        D_RW(sibling)->hdr.switch_counter = rand() % 100;
        sibling = D_RO(sibling)->hdr.sibling_ptr;
      }
      leftmost.oid.off = (uint64_t)D_RO(leftmost)->hdr.leftmost_ptr;
    } while (leftmost.oid.off != 0);
  }
}
