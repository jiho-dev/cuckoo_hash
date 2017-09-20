
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <malloc.h>
#include <errno.h>

#include "cuckoo.h"
#include "cuckoo_malloc.h"
#include "MurmurHash3.h"

#include "benchmark.h"

/////////////////////////////////////////////

static inline
size_t index_hash(const uint32_t hv, const uint32_t hashpower)
{
	return hv >> (32 - hashpower);
}

static inline
size_t alt_index(const size_t index, const tag_t tag, const uint64_t tagmask, const uint64_t hashmask)
{
	// 0x5bd1e995 is the hash constant from MurmurHash3
	return (index ^ ((tag & tagmask) * 0x5bd1e995)) & hashmask;
}

static inline
tag_t tag_hash(const uint32_t hv, const uint64_t tagmask)
{
	uint32_t r = hv & tagmask;

	return (tag_t)r + (r == 0);
}

static inline size_t keyver_index(const size_t i1, const size_t i2) 
{
    return i1 <  i2 ? i1 : i2;
}
/////////////////////////////////////////////////

static void fg_lock(cuckoo_hash_table_t *cukht, uint32_t i1, uint32_t i2)
{
	uint32_t j1, j2;

	j1 = i1 & FG_LOCK_MASK;
	j2 = i2 & FG_LOCK_MASK;

	//__builtin_prefetch((const void*)&cukht->fg_locks[j1], 0, 1);
	//__builtin_prefetch((const void*)&cukht->fg_locks[j2], 0, 1);

	if (j1 < j2) {
		pthread_spin_lock(&cukht->fg_locks[j1]);
		pthread_spin_lock(&cukht->fg_locks[j2]);
	}
	else if (j1 > j2) {
		pthread_spin_lock(&cukht->fg_locks[j2]);
		pthread_spin_lock(&cukht->fg_locks[j1]);
	}
	else {
		pthread_spin_lock(&cukht->fg_locks[j1]);
	}
}

static void fg_unlock(cuckoo_hash_table_t *cukht, uint32_t i1, uint32_t i2)
{
	uint32_t j1, j2;

	j1 = i1 & FG_LOCK_MASK;
	j2 = i2 & FG_LOCK_MASK;

	if (j1 < j2) {
		pthread_spin_unlock(&cukht->fg_locks[j2]);
		pthread_spin_unlock(&cukht->fg_locks[j1]);
	}
	else if (j1 > j2) {
		pthread_spin_unlock(&cukht->fg_locks[j1]);
		pthread_spin_unlock(&cukht->fg_locks[j2]);
	}
	else {
		pthread_spin_unlock(&cukht->fg_locks[j1]);
	}
}

int32_t dummy_cmp_key(const void *key1, const void *key2, const size_t nkey)
{
#if 0
	if (keycmp((const char *)key1, (const char *)key2, nkey)) {
		return 0;
	}
#else
	if (key1 == NULL || key2 == NULL || nkey == 0) {
	}
#endif

	return -1;
}

#if 0
/*
 * Try to read bucket i and check if the given tag is there
 */
static void* try_read(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey, tag_t tag, size_t i)
{
	volatile uint32_t tmp = *((uint32_t *)&(cukht->buckets[i]));
	size_t j;

	for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
		uint8_t ch = ((uint8_t *)&tmp)[j];

		if (ch != tag) {
			continue;
		}

		/* volatile __m128i p, q; */
		/* p = _mm_loadu_si128((__m128i const *) &buckets[i].slots[0]); */
		/* q = _mm_loadu_si128((__m128i const *) &buckets[i].slots[2]); */
		/* void *slots[4]; */

		/* _mm_storeu_si128((__m128i *) slots, p); */
		/* _mm_storeu_si128((__m128i *) (slots + 2), q); */
		/* void *data = slots[j]; */

		void *data = cukht->buckets[i].slots[j];

		if (cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {
			return data;
		}
	}

	return NULL;
}
#endif

/*
 * Try to add an void to bucket i,
 * return true on success and false on failure
 */
static int32_t try_add(cuckoo_hash_table_t *cukht, void *data, tag_t tag, size_t i, size_t lock)
{
	size_t j;
#if 0
	uint32_t vs, ve;

TRY_START:
	vs = READ_KEYVER(cukht, lock);

	// odd means that key is under referenced by the other
	if (vs & 1)
		goto TRY_START;
#endif

	for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
#if 0
		ve = READ_KEYVER(cukht, lock);
#endif

		if (IS_SLOT_EMPTY(cukht, i, j)) {
#if 0
			// still safe ?
			if ((ve & 1) || vs != ve)
				goto TRY_START;
#endif

			// make the key odd
			INC_KEYVER(cukht, lock);

#ifdef CUCKOO_LOCK_FINEGRAIN
			fg_lock(cukht, i, i);
#endif

			cukht->buckets[i].tags[j] = tag;
			cukht->buckets[i].slots[j] = data;
			cukht->num_items++;

			// make the key even
			INC_KEYVER(cukht, lock);

#ifdef CUCKOO_LOCK_FINEGRAIN
			fg_unlock(cukht, i, i);
#endif
			return 1;
		}
	}

	return 0;
}

static void* try_del(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey, tag_t tag, size_t i, size_t lock)
{
	size_t j;
	void *data = NULL;
#if 0
	uint32_t vs, ve;

TRY_START:
	vs = READ_KEYVER(cukht, lock);

	if (vs & 1)
		goto TRY_START;
#endif

	for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
#if 0
		ve = READ_KEYVER(cukht, lock);
#endif

		if (!IS_TAG_EQUAL(cukht, i, j, tag)) {
			continue;
		}

		data = cukht->buckets[i].slots[j];

		if (data == NULL) {
			// found but no data
			return NULL;
		}

#if 0
		// still safe ?
		if ((ve & 1) || vs != ve)
			goto TRY_START;
#endif

		// call _compare_key()
		if (cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {

#ifdef CUCKOO_LOCK_FINEGRAIN
			fg_lock(cukht, i, i);
#endif

			INC_KEYVER(cukht, lock);

			cukht->buckets[i].tags[j] = 0;
			cukht->buckets[i].slots[j] = 0;
			cukht->num_items--;

			INC_KEYVER(cukht, lock);

#ifdef CUCKOO_LOCK_FINEGRAIN
			fg_unlock(cukht, i, i);
#endif

			return data;
		}
	}

	return RET_PTR_ERR;
}

/*
 * Make bucket  from[idx] slot[whichslot] available to insert a new void
 * return idx on success, -1 otherwise
 * @param from:   the array of bucket index
 * @param whichslot: the slot available
 * @param  depth: the current cuckoo depth
 */
static int32_t path_search(cuckoo_hash_table_t *cukht, size_t depth_start, size_t *cp_index)
{
	int32_t depth = depth_start;

	while ((cukht->num_kick < MAX_CUCKOO_COUNT) &&
		   (depth >= 0) && (depth < MAX_CUCKOO_COUNT - 1)) {
		size_t *from = &(cukht->cuk_path[depth].cp_buckets[0]);
		size_t *to = &(cukht->cuk_path[depth + 1].cp_buckets[0]);

		/*
		 * Check if any slot is already free
		 */
		size_t idx;
		for (idx = 0; idx < CUCKOO_WIDTH; idx++) {
			size_t i = from[idx];
			size_t j;
			for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
				if (IS_SLOT_EMPTY(cukht, i, j)) {
					cukht->cuk_path[depth].cp_slot_idxs[idx] = j;
					*cp_index = idx;

					return depth;
				}
			}

			// pick the victim item
			cukht->idx_victim++;
			j = cukht->idx_victim % BUCKET_SLOT_SIZE;

			cukht->cuk_path[depth].cp_slot_idxs[idx] = j;
			cukht->cuk_path[depth].cp_slots[idx] = cukht->buckets[i].slots[j];
			to[idx] = alt_index(i, cukht->buckets[i].tags[j],
								cukht->tag_mask, cukht->hash_mask);
		}

		cukht->num_kick += CUCKOO_WIDTH;
		depth++;
	}

	return -1;
}

static int32_t move_backward(cuckoo_hash_table_t *cukht, size_t depth_start, size_t idx)
{
	int32_t depth = depth_start;

	while (depth > 0) {
		size_t i1 = cukht->cuk_path[depth - 1].cp_buckets[idx];
		size_t i2 = cukht->cuk_path[depth].cp_buckets[idx];

		size_t j1 = cukht->cuk_path[depth - 1].cp_slot_idxs[idx];
		size_t j2 = cukht->cuk_path[depth].cp_slot_idxs[idx];

		/*
		 * We plan to kick out j1, but let's check if it is still there;
		 * there's a small chance we've gotten scooped by a later cuckoo.
		 * If that happened, just... try again.
		 */

		if (cukht->buckets[i1].slots[j1] !=
			cukht->cuk_path[depth - 1].cp_slots[idx]) {
			/* try again */
			return depth;
		}

		if (IS_SLOT_EMPTY(cukht, i2, j2)) {
			size_t lock   = keyver_index(i1, i2);
			INC_KEYVER(cukht, lock);

#ifdef CUCKOO_LOCK_FINEGRAIN
			fg_lock(cukht, i1, i2);
#endif

			cukht->buckets[i2].tags[j2] = cukht->buckets[i1].tags[j1];
			cukht->buckets[i2].slots[j2] = cukht->buckets[i1].slots[j1];

			cukht->buckets[i1].tags[j1] = 0;
			cukht->buckets[i1].slots[j1] = 0;

			cukht->num_moves++;

			INC_KEYVER(cukht, lock);

#ifdef CUCKOO_LOCK_FINEGRAIN
			fg_unlock(cukht, i1, i2);

#endif
			depth--;
		}
	}

	return depth;
}

static int32_t cuckoo(cuckoo_hash_table_t *cukht, int32_t depth)
{
	int32_t cur;
	size_t idx;

	cukht->num_kick = 0;

	while (1) {
		cur = path_search(cukht, depth, &idx);
		if (cur < 0) {
			return -1;
		}

		cur = move_backward(cukht, cur, idx);
		if (cur == 0) {
			return idx;
		}

		depth = cur - 1;
	}

	return -1;
}

///////////////////////////////////////////////////////

cuckoo_hash_table_t *
cuckoo_init_hash_table(const int32_t hashtable_init, cuckoo_cmp_key cmp_key)
{
	cuckoo_hash_table_t *cukht;
	int32_t len = sizeof(cuckoo_hash_table_t);

	cukht = cuckoo_malloc(len);
	if (cukht == NULL) {
		return NULL;
	}

	memset(cukht, 0, len);

	cukht->hash_power = HASHPOWER_DEFAULT;
	if (hashtable_init) {
		cukht->hash_power = hashtable_init;
	}

	cukht->hash_size = (uint64_t)1 << (cukht->hash_power);
	cukht->hash_mask = cukht->hash_size - 1;

	/*
	 * tagpower: number of bits per tag
	 */
	cukht->tag_power = sizeof(tag_t) * 8;
	cukht->tag_mask = ((uint64_t)1 << cukht->tag_power) - 1;

	len = cukht->hash_size * sizeof(cuckoo_bucket_t);

	cukht->buckets = cuckoo_malloc(len);
	if (cukht->buckets == NULL) {
		goto FAIL;
	}

	memset(cukht->buckets, 0, len);

	len = sizeof(cuckoo_path_t) * MAX_CUCKOO_COUNT;
	cukht->cuk_path = cuckoo_malloc(len);
	if (cukht->cuk_path == NULL) {
		goto FAIL;
	}

	memset(cukht->cuk_path, 0, len);

	len = sizeof(cuckoo_spinlock_t) * FG_LOCK_COUNT;
	cukht->fg_locks = cuckoo_malloc(len);
	if (cukht->fg_locks == NULL) {
		goto FAIL;
	}

	size_t i;
	for (i = 0; i < FG_LOCK_COUNT; i++) {
		pthread_spin_init(&cukht->fg_locks[i], PTHREAD_PROCESS_PRIVATE);
	}

	pthread_spin_init(&cukht->wlock, PTHREAD_PROCESS_PRIVATE);

    memset(cukht->keyver_array, 0, sizeof(uint32_t) * KEYVER_COUNT);

	cukht->cb_cmp_key = dummy_cmp_key;
	if (cmp_key) {
		cukht->cb_cmp_key = cmp_key;
	}

	return cukht;

FAIL:
	if (cukht) {
		if (cukht->buckets) {
			cuckoo_free(cukht->buckets);
		}

		if (cukht->cuk_path) {
			cuckoo_free(cukht->cuk_path);
		}

		if (cukht->fg_locks) {
			cuckoo_free((void *)cukht->fg_locks);
		}

		cuckoo_free(cukht);
	}

	return NULL;
}

void cuckoo_destroy_hash_table(cuckoo_hash_table_t *cukht)
{
	if (cukht->buckets) {
		cuckoo_free(cukht->buckets);
	}

	if (cukht->cuk_path) {
		cuckoo_free(cukht->cuk_path);
	}

	if (cukht->fg_locks) {
		cuckoo_free((void *)cukht->fg_locks);
	}

	cuckoo_free(cukht);
}

uint32_t cuckoo_hash(const char *key, const uint32_t len)
{
	uint32_t hash = 0;

	MurmurHash3_x86_32((const void*)key, (int32_t)len, (uint32_t)0x43606326, (void *)&hash);

	return hash;
}

static inline
uint32_t cuckoo_read_even(cuckoo_hash_table_t *cukht, size_t lock)
{
	uint32_t c;

	do {
		c = READ_KEYVER(cukht, lock);
	} while (c & 1);

	return c;
}

static inline int32_t cuckoo_find_slot(uint32_t tags, tag_t tag)
{
	int32_t j;

	for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
		if (tag == ((tag_t *)&tags)[j]) {
			return j;
		}
	}

	return -1;
}

static inline int32_t cuckoo_changed(cuckoo_hash_table_t *cukht, size_t lock, uint32_t c)
{
	return (c != READ_KEYVER(cukht, lock));

}

static inline 
void* cuckoo_find_data(cuckoo_hash_table_t *cukht, tag_t tag, size_t idx, size_t kidx, const void *key, uint32_t nkey) 
{
	void *result = NULL, *data;
	uint32_t vs;
	volatile uint32_t tags;
	int32_t j;

	do {
		vs = cuckoo_read_even(cukht, kidx);
		//__builtin_prefetch(&(cukht->buckets[idx]));
		tags = *((uint32_t *)&(cukht->buckets[idx]));
		j = cuckoo_find_slot(tags, tag);

		if (cuckoo_changed(cukht, kidx, vs)) {
			continue;
		}
		else if (j < 0) {
			return NULL;
		}

		data = cukht->buckets[idx].slots[j];
		__builtin_prefetch(data);

		// call _compare_key()
		if (data && 
			cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {
			result = data;
		}

	} while(cuckoo_changed(cukht, kidx, vs));

	return result;
}

void* cuckoo_find(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey, const uint32_t hash)
{
	tag_t tag;
	size_t i1, i2;
	void *result = NULL;
	size_t kidx;

	//hash = cuckoo_hash(key, nkey);
	tag = tag_hash(hash, cukht->tag_mask);
	i1 = index_hash(hash, cukht->hash_power);
	i2 = alt_index(i1, tag, cukht->tag_mask, cukht->hash_mask);
	kidx = keyver_index(i1, i2);

	result = cuckoo_find_data(cukht, tag, i1, kidx, key, nkey);
	if (result != NULL) {
		return result;
	}

	result = cuckoo_find_data(cukht, tag, i2, kidx, key, nkey);

	return result;
}

/*
 * The interface to find a key in this hash table
 */
void* cuckoo_find1(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey, const uint32_t hash)
{
	tag_t tag;
	size_t i1, i2;

	//hash = cuckoo_hash(key, nkey);
	tag = tag_hash(hash, cukht->tag_mask);
	i1 = index_hash(hash, cukht->hash_power);
	i2 = alt_index(i1, tag, cukht->tag_mask, cukht->hash_mask);

	void *result = NULL;
	size_t lock = keyver_index(i1, i2);
	uint32_t vs, ve;
	size_t j;
	volatile uint32_t tags1, tags2;

TRY_AGAIN:

#ifdef CUCKOO_LOCK_FINEGRAIN
	fg_lock(cukht, i1, i2);
#endif

	//vs = READ_KEYVER(cukht, lock);
	vs = cuckoo_read_even(cukht, lock);
	tags1 = *((uint32_t *)&(cukht->buckets[i1]));

	//if (vs & 1) 
	//	goto TRY_AGAIN;

	for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
		uint8_t ch = ((uint8_t *)&tags1)[j];
		if (ch != tag) {
			continue;
		}

		ve = cuckoo_read_even(cukht, lock);
		//ve = READ_KEYVER(cukht, lock);
		void *data = cukht->buckets[i1].slots[j];
		__builtin_prefetch(data);
		if (data == NULL) {
			continue;
		}

		//if ((ve & 1) || vs != ve)
		if (vs != ve)
			goto TRY_AGAIN;

		// call _compare_key()
		if (cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {
			result = data;
			goto END;
		}
	}

	if (!result) {
		//__builtin_prefetch(&(cukht->buckets[i2]));
		tags2 = *((uint32_t *)&(cukht->buckets[i2]));

		for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
			uint8_t ch = ((uint8_t *)&tags2)[j];
			if (ch != tag) {
				continue;
			}

			//ve = READ_KEYVER(cukht, lock);
			ve = cuckoo_read_even(cukht, lock);
			void *data = cukht->buckets[i2].slots[j];
			__builtin_prefetch(data);
			if (data == NULL) {
				continue;
			}

			//if (ve & 1 || vs != ve)
			if (vs != ve)
				goto TRY_AGAIN;

			// call _compare_key()
			if (cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {
				result = data;
				break;
			}
		}
	}

END:

	//ve = READ_KEYVER(cukht, lock);
	ve = cuckoo_read_even(cukht, lock);
	//if (ve & 1 || vs != ve)
	if (vs != ve)
		goto TRY_AGAIN;

#ifdef CUCKOO_LOCK_FINEGRAIN
	fg_unlock(cukht, i1, i2);
#endif

	return result;
}

// need to be protected by cache_lock
void* cuckoo_delete(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey, const uint32_t hash)
{
	tag_t tag;
	size_t i1, i2;
	void *data;

	//hash = cuckoo_hash(key, nkey);
	tag = tag_hash(hash, cukht->tag_mask);
	i1 = index_hash(hash, cukht->hash_power);
	i2 = alt_index(i1, tag, cukht->tag_mask, cukht->hash_mask);
	size_t lock = keyver_index(i1, i2);

	pthread_spin_lock(&cukht->wlock);

	data = try_del(cukht, key, nkey, tag, i1, lock);
	if (data != RET_PTR_ERR) {
		pthread_spin_unlock(&cukht->wlock);
		return data;
	}

	data = try_del(cukht, key, nkey, tag, i2, lock);
	pthread_spin_unlock(&cukht->wlock);

	return data;
}

// need to be protected by cache_lock
int32_t cuckoo_insert(cuckoo_hash_table_t *cukht, const uint32_t hash, void *data)
{
	tag_t tag;
	size_t i1, i2;

	//hash = cuckoo_hash(key, klen);
	tag = tag_hash(hash, cukht->tag_mask);
	i1 = index_hash(hash, cukht->hash_power);
	i2 = alt_index(i1, tag, cukht->tag_mask, cukht->hash_mask);
	size_t lock = keyver_index(i1, i2);

	//fg_lock(cukht, i1, i2);
	pthread_spin_lock(&cukht->wlock);

	if (try_add(cukht, data, tag, i1, lock) ||
		try_add(cukht, data, tag, i2, lock)) {
		pthread_spin_unlock(&cukht->wlock);
		//fg_unlock(cukht, i1, i2);
		return 1;
	}

	//fg_unlock(cukht, i1, i2);
	//pthread_spin_lock(&cukht->wlock);

	int32_t idx;
	size_t depth = 0;
	for (idx = 0; idx < CUCKOO_WIDTH; idx++) {
		if (idx < CUCKOO_WIDTH / 2) {
			cukht->cuk_path[depth].cp_buckets[idx] = i1;
		}
		else {
			cukht->cuk_path[depth].cp_buckets[idx] = i2;
		}
	}

	size_t j;
	idx = cuckoo(cukht, depth);
	if (idx >= 0) {
		i1 = cukht->cuk_path[depth].cp_buckets[idx];
		j = cukht->cuk_path[depth].cp_slot_idxs[idx];

		if (cukht->buckets[i1].slots[j] == 0 &&
			try_add(cukht, data, tag, i1, lock)) {
			pthread_spin_unlock(&cukht->wlock);
			return 1;
		}
	}

	pthread_spin_unlock(&cukht->wlock);

	cukht->num_error++;

#if 0
	printf("hash table is full (hashpower = %d, \
		num_items = %u, load factor = %.2f), \
		need to increase hashpower\n",
		   cukht->hash_power,
		   cukht->num_items,
		   1.0 * cukht->num_items / BUCKET_SLOT_SIZE / cukht->hash_size);
#endif

	return 0;
}

/////////////////////////////////////////////////

#if 0
void assoc2_post_bench()
{
	size_t total_size = 0;

	printf("num_items = %u\n", num_items);
	printf("index table size = %zu\n", hashsize);
	printf("hashtable size = %zu KB\n", hashsize * sizeof(cuckoo_bucket_t) / 1024);
	printf("hashtable load factor= %.5f\n", 1.0 * num_items / BUCKET_SLOT_SIZE / hashsize);
	total_size += hashsize * sizeof(cuckoo_bucket_t);
	printf("total_size = %zu KB\n", total_size / 1024);
	printf("moves per insert = %.2f\n", (double)num_moves / num_items);
	printf("\n");
}

#endif


static int fglock_compare_key(const void *key1, const void *key2, const size_t nkey)
{
	const char *_key1 = (const char *)key1;
	//const char *_key2 = (const char *)key2;
	const char *_key2;
#if 0
	cuckoo_item_t *it = (cuckoo_item_t*)key2;
	_key2 = (const char*)it->key;

	if (nkey != it->key_len) {
		return -1;
	}


	char **pp = (char**)it;
	char *p = *pp;
	printf("it=%p, %p, key=%p, %p \n", it, pp,  it->key, p );
#else
	char **pp = (char**)key2;
	_key2 = (const char*)*pp;
#endif

	return memcmp(_key1, _key2, nkey);
}

static cuckoo_item_t* fglock_alloc_item(const char *key, const char *val, int len)
{

	cuckoo_item_t *it = cuckoo_malloc(sizeof(cuckoo_item_t));
	
	if (it == NULL) {
		return NULL;
	}

	it->key = strdup(key);
	it->key_len = len;
	it->value = strdup(val);

	return it;
}

static void fglock_free_item(cuckoo_item_t *it)
{
	if (it == NULL || it == RET_PTR_ERR) {
		return;
	}

	cuckoo_free((void*)it->key);
	cuckoo_free(it->value);
	cuckoo_free(it);
}

static void* fglock_bench_init_hash_table(word_list_t *wordlist)
{
	cuckoo_hash_table_t *fglock_ht;
	int i;

	///////////////////////////
	// init hash entries
	for (i = 0; i < wordlist->word_cnt; i++) {
		char *key = wordlist->word[i];
		char *val = key;

		cuckoo_item_t* it = fglock_alloc_item(key, val, wordlist->lens[i]);
		wordlist->items[i] = (void*)it;
	}

	///////////////////////////
	// init hash tables
	fglock_ht = cuckoo_init_hash_table(23, fglock_compare_key);

	printf("Hashtable size: %lu\n", fglock_ht->hash_size);
	fflush(NULL);

	return (void*)fglock_ht;
}

static void fglock_bench_print_hash_table_info(void *_ht, char *msg)
{
	cuckoo_hash_table_t *fglock_ht = (cuckoo_hash_table_t*)_ht;

	printf("%s key pairs: %d \n", msg, fglock_ht->num_items);
	printf("Cuckoo Movements: %d \n", fglock_ht->num_moves);
	fflush(NULL);
}

static int fglock_bench_insert_item(void *_ht, const char *key, const size_t len, void *val, uint32_t hash, void *data)
{
	cuckoo_hash_table_t *fglock_ht = (cuckoo_hash_table_t*)_ht;

	int ret;

	ret = cuckoo_insert(fglock_ht, hash, data);

	return ret;
}

static int fglock_bench_alloc_insert_data(void *_ht, const char *key, const size_t len, void *val, uint32_t hash)
{
	cuckoo_item_t* it = fglock_alloc_item(key, val, len);
	
	if (it) {
		fglock_bench_insert_item(_ht,key, len, val, hash, it);
		return 0;
	}

	return -1;
}

static int fglock_bench_search_item(void *_ht, const char *key, const size_t len, uint32_t hash)
{
	cuckoo_hash_table_t *fglock_ht = (cuckoo_hash_table_t*)_ht;
	cuckoo_item_t *it;

	it = cuckoo_find(fglock_ht, key, len, hash);
#if 0
	if (it) {
		val1 = (char *)it->value;
	}
#endif

	if (it != NULL 
		//&& strncmp(val, val1, klen) != 0
		) {
		return 0;
	}

	return -1;
}

static void* fglock_bench_delete_item(void *_ht, const char *key,  const size_t len, uint32_t hash)
{
	cuckoo_hash_table_t *fglock_ht = (cuckoo_hash_table_t*)_ht;
	cuckoo_item_t *it;

	it = cuckoo_delete(fglock_ht, key, len, hash);

	if (it != RET_PTR_ERR && it != NULL) {
		fglock_free_item(it);
	}

	return it;
}

static void fglock_bench_clean_hash_table(void *_ht)
{
	cuckoo_hash_table_t *fglock_ht = (cuckoo_hash_table_t*)_ht;

	cuckoo_destroy_hash_table(fglock_ht);
}

//////////////////////////////////

cuckoo_bench_t fglock_bench = {
	.name = "FGLOCK_HASHTABLE",
	
	.bench_init_hash_table = fglock_bench_init_hash_table,
	.bench_clean_hash_table = fglock_bench_clean_hash_table,

	.bench_insert_item = fglock_bench_insert_item,
	.bench_alloc_insert_data = fglock_bench_alloc_insert_data,
	.bench_search_item = fglock_bench_search_item,
	.bench_delete_item = fglock_bench_delete_item,
	
	.bench_print_hash_table_info = fglock_bench_print_hash_table_info,
};
