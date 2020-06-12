#include "btree.h"

/*
 *file_exists -- checks if file exists
 */
static inline int file_exists(char const *file) { return access(file, F_OK); }

void clear_cache() {
  // Remove cache
  int size = 256 * 1024 * 1024;
  char *garbage = new char[size];
  for (int i = 0; i < size; ++i)
    garbage[i] = i;
  for (int i = 100; i < size; ++i)
    garbage[i] += garbage[i - 100];
  delete[] garbage;
}

// MAIN
int main(int argc, char **argv) {
  // Parsing arguments
  int num_data = 0;
  int n_threads = 1;
  float selection_ratio = 0.0f;
  char *input_path = (char *)std::string("../sample_input.txt").data();
  char *persistent_path;

  srand(time(NULL));
  int c;
  while ((c = getopt(argc, argv, "n:w:t:s:i:p:")) != -1) {
    switch (c) {
    case 'n':
      num_data = atoi(optarg);
      break;
    case 't':
      n_threads = atoi(optarg);
      break;
    case 's':
      selection_ratio = atof(optarg);
    case 'i':
      input_path = optarg;
    case 'p':
      persistent_path = optarg;
    default:
      break;
    }
  }

  int selected = num_data * selection_ratio;
  // Make or Read persistent pool
  TOID(btree) bt = TOID_NULL(btree);
  PMEMobjpool *pop;

  if (file_exists(persistent_path) != 0) {
    pop = pmemobj_create(persistent_path, "btree", 8000000000,
                         0666); // make 1GB memory pool
    bt = POBJ_ROOT(pop, btree);
    D_RW(bt)->constructor(pop);
  } else {
    pop = pmemobj_open(persistent_path, "btree");
    bt = POBJ_ROOT(pop, btree);
  }

  struct timespec start, end;

  // Reading data
  entry_key_t *keys = new entry_key_t[num_data];
  entry_key_t *query = new entry_key_t[2000];
  unsigned long *bufs = new unsigned long[num_data];

  ifstream ifs;
  ifs.open(input_path);
  if (!ifs) {
    cout << "input loading error!" << endl;

    delete[] keys;
    exit(-1);
  }

  for (int i = 0; i < num_data; ++i) {
    ifs >> keys[i];
  }

  ifs.close();

  ifs.open("../workload/number1.txt");
  if (!ifs) {
    cout << "query loading error!" << endl;

    delete[] query;
    exit(-1);
  }

  for (int i = 0; i < 2000; ++i) {
    ifs >> query[i];
  }

  ifs.close();

  {
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (int i = 0; i < num_data; ++i) {
      D_RW(bt)->btree_insert(keys[i], (char *)keys[i]);
    }

    clock_gettime(CLOCK_MONOTONIC, &end);

    long long elapsed_time = (end.tv_sec - start.tv_sec) * 1000000000 +
                             (end.tv_nsec - start.tv_nsec);
    elapsed_time /= 1000;

    printf("INSERT elapsed_time: %ld, Avg: %f\n", elapsed_time,
           (double)elapsed_time / num_data);
    //    D_RW(bt)->printAll();
  }
  int Dead = 0;

  for (int i = 0; i < Dead; ++i) {
    D_RW(bt)->btree_delete(keys[i]);
  }

  //  Search performance of F&F are overestimated in insertion only cases.
  //  If you want to see average search performance, enable following line
  //  D_RW(bt)->randScounter();

  clear_cache();

  {
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (int i = Dead; i < num_data; ++i) {
      D_RW(bt)->btree_search(keys[i]);
    }

    clock_gettime(CLOCK_MONOTONIC, &end);

    long long elapsed_time = (end.tv_sec - start.tv_sec) * 1000000000 +
                             (end.tv_nsec - start.tv_nsec);
    elapsed_time /= 1000;

    printf("SEARCH elapsed_time: %ld, Avg: %f\n", elapsed_time,
           (double)elapsed_time / (num_data - Dead));
  }

  clear_cache();

  {
    clock_gettime(CLOCK_MONOTONIC, &start);

    int k = 1000;
    for (int i = 0; i < k; i++) {
      if (query[i] + selected < 100000000)
        D_RW(bt)->btree_search_range(query[i], query[i] + selected, bufs);
      else
        k++;
    }

    clock_gettime(CLOCK_MONOTONIC, &end);

    long long elapsed_time = (end.tv_sec - start.tv_sec) * 1000000000 +
                             (end.tv_nsec - start.tv_nsec);
    elapsed_time /= 1000;

    printf("Range SEARCH elapsed_time: %ld, Avg: %f\n", elapsed_time,
           (double)elapsed_time / num_data);
  }

  delete[] keys;
  delete[] query;
  delete[] bufs;

  pmemobj_close(pop);
  return 0;
}
