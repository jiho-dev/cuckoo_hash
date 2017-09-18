#ifndef __CUCKOO_H__
#define __CUCKOO_H__


#include <stdint.h>


#define CUCKOO_HASH_SIZE    64

#if (CUCKOO_HASH_SIZE == 32)
typedef uint32_t cuckoo_hashvalue_t;
#elif (CUCKOO_HASH_SIZE == 64)
typedef uint64_t cuckoo_hashvalue_t;
#else
#error "Unsupported CUCKOO_HASH_SIZE"
#endif

/* 2**X */
#define POW2(X) ((size_t)1 << (X))

typedef void *cuckoo_key_t;
typedef void *cuckoo_value_t;

#define CUCKOO_NUM_HASH 2

typedef struct cuckoo_key_s {
	cuckoo_hashvalue_t hash[CUCKOO_NUM_HASH];
} cuckoo_hashkey_t;

typedef struct cuckoo_elem_s {
	cuckoo_hashkey_t	hashkey;

	cuckoo_key_t		key;
	uint32_t			keylen;
	cuckoo_value_t		value;
} cuckoo_elem_t;


#define CUCKOO_STASH_SIZE   16
#define CUCKOO_BIN_SIZE     4
// 10 이상은 별로 효과가 없고 성능만 저하 시킨다.
#define CUCKOO_MAX_LOOP     200

typedef struct cuckoo_stash_s {
	cuckoo_elem_t	elements[CUCKOO_STASH_SIZE];
	uint16_t		used;
	uint16_t		dummy;
} cuckoo_stash_t;

typedef struct cuckoo_hashtable_s {
	uint32_t		element_size;
	uint32_t		bucket_size;
	uint32_t		used;
	uint16_t		max_loops;
	uint16_t		bin_size;

	cuckoo_stash_t	stash;

	// this must be the last members
	cuckoo_elem_t	*elements;
} cuckoo_hashtable_t;



/////////////////////////////////////////////////

cuckoo_hashtable_t* cuckoo_alloc_hashtable(size_t shift);
void cuckoo_free_hashtable(cuckoo_hashtable_t *ht);
int cuckoo_add_element(cuckoo_hashtable_t *ht, cuckoo_key_t key, uint32_t klen, cuckoo_value_t value);
int cuckoo_delete_element(cuckoo_hashtable_t *ht, cuckoo_key_t key, uint32_t klen);
cuckoo_value_t cuckoo_lookup_element(cuckoo_hashtable_t *ht, cuckoo_key_t key, uint32_t klen);



#endif
