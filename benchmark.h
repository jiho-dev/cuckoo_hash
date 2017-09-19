#ifndef __BENCHMARK_H__
#define __BENCHMARK_H__

typedef struct word_list_s {
	char	**word;
	int 	*lens;
	uint32_t *hash;
	void 	**items;

	int		alloc_cnt;
	int		word_cnt;
} word_list_t;

typedef struct cuckoo_bench_s {
	char *name;

	void* (*bench_init_hash_table)(word_list_t *wordlist);
	void  (*bench_clean_hash_table)(void *_ht);

	int   (*bench_insert_item)(void *_ht, const char *key, const size_t len, void *val, uint32_t hash, void *data);
	int   (*bench_search_item)(void *_ht, const char *key, const size_t len, uint32_t hash);
	void* (*bench_delete_item)(void *_ht, const char *key,  const size_t len, uint32_t hash);
	int   (*bench_alloc_insert_data)(void *_ht, const char *key, const size_t len, void *val, uint32_t hash);

	void  (*bench_print_hash_table_info)(void *_ht, char *msg);
} cuckoo_bench_t;


typedef struct thread_arg_s {
	int idx;
	int act;
	int nwait;
	int round;

	word_list_t *w;
	void 		*hashtable;
	cuckoo_bench_t *bm;

} thread_arg_t;

typedef void *(*thread_function) (void *);



///////////////////////

extern cuckoo_bench_t fglock_bench;
extern cuckoo_bench_t slot_bench;

#endif
