#ifndef __LOCKFREE_CUCKOO_HASH_TABLE_H__
#define __LOCKFREE_CUCKOO_HASH_TABLE_H__


#define THRESHOLD   50
#define MAX_DEPTH 	256

#include <stdint.h>

///////////////////////////////////////////////////

typedef struct hash_entry_s {
	char 		*key;
	char 		*val;
	uint32_t 	klen;
} hash_entry_t;

typedef hash_entry_t *count_ptr_t;

enum FIND_RESULT { FIRST, SECOND, NIL };

typedef struct hash_table_s {
	count_ptr_t 	*table[2];
	
	uint32_t		used;
	uint32_t		size1;
	uint32_t		size2;

	hash_entry_t 	*rlist[MAX_DEPTH][MAX_DEPTH];
	hash_entry_t 	*hp_rec[MAX_DEPTH][2];
	uint32_t		rcount[MAX_DEPTH];
} hash_table_t;


///////////////////////////

#define hash_cmpchg(dest, old, _new) \
	__sync_bool_compare_and_swap((uint64_t*)dest, (uint64_t)old, (uint64_t)_new)


#define set_mark(dest, old) \
	hash_cmpchg(dest, old, set_marked(old, 1))

#define set_pointer(dest, old, _new) \
	hash_cmpchg(dest, old, make_pointer(_new, get_counter(old)))

#define	reset_pointer(dest, old) \
	hash_cmpchg(dest, old, make_pointer(NULL, get_counter(old)))

#define set_pointer_count(dest, old, _new, cnt) \
	hash_cmpchg(dest, old, make_pointer(_new, cnt))

//////////////////////////////////////////////

////////////////
//
int lfckht_insert(hash_table_t *ht, char *key, uint32_t klen, char *val, uint32_t tid);
hash_table_t *lfckht_init_hash_table(uint32_t capacity, uint32_t thread_count);
int lfckht_search(hash_table_t *ht, char *key, uint32_t klen, uint32_t tid, char **val);

#endif
