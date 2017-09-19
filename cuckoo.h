#ifndef __CUCKOO_H__
#define __CUCKOO_H__

#include <stdint.h>
#include <pthread.h>

/////////////////////////////////

/*
 * enable huge table to reduce TLB misses
 */
//#define CUCKOO_ENABLE_HUGEPAGE

/*
 * enable parallel cuckoo
 */
#define CUCKOO_WIDTH 1




/*
 * enable bucket locking
 */
//#define MEMC3_LOCK_FINEGRAIN    1

/*
 * enable optimistic locking
 */
#define MEMC3_LOCK_OPT    1


#if (MEMC3_LOCK_OPT + MEMC3_LOCK_FINEGRAIN + MEMC3_LOCK_GLOBAL + MEMC3_LOCK_NONE != 1)
#error "you must specify one and only locking policy"
#endif


////////////////////////////////

/*
 * The maximum number of cuckoo operations per insert,
 * we use 128 in the submission
 * now change to 500
 */
#define MAX_CUCKOO_COUNT 500

/*
 * the structure of a bucket
 */
#define BUCKET_SLOT_SIZE 4

#define FG_LOCK_COUNT 8192
#define FG_LOCK_MASK (FG_LOCK_COUNT - 1)

/* Initial power multiplier for the hash table */
#define HASHPOWER_DEFAULT 25
//#define HASHPOWER_DEFAULT 16

#define IS_SLOT_EMPTY(cukht, i, j) (cukht->buckets[i].tags[j] == 0)
#define IS_TAG_EQUAL(cukht, i, j, tag) ((cukht->buckets[i].tags[j] & cukht->tag_mask) == tag)

#define RET_PTR_ERR 	((void*)-1)


//#ifdef MEMC3_LOCK_OPT
//  keyver array has 8192 buckets,
#define KEYVER_COUNT 		((unsigned long int)1 << (13))
#define KEYVER_MASK  		(KEYVER_COUNT - 1)
#define READ_KEYVER(cukht, lock) 	__sync_fetch_and_add(&cukht->keyver_array[lock & KEYVER_MASK], 0)
#define INC_KEYVER(cukht, lock) 	__sync_fetch_and_add(&cukht->keyver_array[lock & KEYVER_MASK], 1)

//#endif

/////////////////////////////////////////////////

typedef int32_t (*cuckoo_cmp_key)(const void *key1, const void *key2, const size_t key_len);
typedef uint8_t tag_t;
typedef __int128_t int128_t;
typedef __uint128_t uint128_t;
typedef pthread_spinlock_t cuckoo_spinlock_t;

typedef struct cuckoo_item_s {
	const void	*key;
	size_t		key_len;
	void		*value;
} cuckoo_item_t ;

#if 0
typedef struct cuckoo_slot_s {
	uint32_t	hash;
	//uint32_t  notused;
	ValueType	data;
}  __attribute__((__packed__)) cuckoo_slot_t;
#else
typedef void *cuckoo_slot_t;
#endif

typedef struct bucket {
	tag_t			tags[BUCKET_SLOT_SIZE];     // 4bytes
	//uint8_t			notused[4];             // ???
	cuckoo_slot_t	slots[BUCKET_SLOT_SIZE];
}  __attribute__((__packed__)) cuckoo_bucket_t;

typedef struct cuckoo_path_s {
	size_t			cp_buckets[CUCKOO_WIDTH];
	size_t			cp_slot_idxs[CUCKOO_WIDTH];
	cuckoo_slot_t	cp_slots[CUCKOO_WIDTH];
} cuckoo_path_t;

typedef struct cuckoo_hashtable_ {
	cuckoo_bucket_t		*buckets;
	cuckoo_cmp_key		cb_cmp_key;
	cuckoo_path_t		*cuk_path;

	cuckoo_spinlock_t	*fg_locks;
	cuckoo_spinlock_t	wlock;
	uint32_t keyver_array[KEYVER_COUNT];


	uint32_t			idx_victim;
	uint32_t			num_error;
	uint32_t			num_kick;
	uint32_t			num_items;
	uint32_t			num_moves;

	uint32_t			hash_power;
	uint64_t			hash_size;
	uint64_t			hash_mask;

	uint64_t			tag_power;
	uint64_t			tag_mask;
} cuckoo_hash_table_t;


/////////////////////////////////////////

cuckoo_hash_table_t* cuckoo_init_hash_table(const int32_t hashpower_init, cuckoo_cmp_key cmp_key);
void cuckoo_destroy_hash_table(cuckoo_hash_table_t *cukht);
void* cuckoo_find(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey, const uint32_t hash);
int32_t cuckoo_insert(cuckoo_hash_table_t *cukht, const uint32_t hash, void *data);
void* cuckoo_delete(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey, const uint32_t hash);
uint32_t cuckoo_hash(const char *key, const uint32_t len);


#endif
