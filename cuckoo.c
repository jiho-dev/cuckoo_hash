
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

static void fg_lock(bcht_t *bcht, uint32_t i1, uint32_t i2)
{
	uint32_t j1, j2;

	j1 = i1 & BCHT_LOCK_MASK;
	j2 = i2 & BCHT_LOCK_MASK;

	//__builtin_prefetch((const void*)&bcht->fg_locks[j1], 0, 1);
	//__builtin_prefetch((const void*)&bcht->fg_locks[j2], 0, 1);

	if (j1 < j2) {
		pthread_spin_lock(&bcht->fg_locks[j1]);
		pthread_spin_lock(&bcht->fg_locks[j2]);
	}
	else if (j1 > j2) {
		pthread_spin_lock(&bcht->fg_locks[j2]);
		pthread_spin_lock(&bcht->fg_locks[j1]);
	}
	else {
		pthread_spin_lock(&bcht->fg_locks[j1]);
	}
}

static void fg_unlock(bcht_t *bcht, uint32_t i1, uint32_t i2)
{
	uint32_t j1, j2;

	j1 = i1 & BCHT_LOCK_MASK;
	j2 = i2 & BCHT_LOCK_MASK;

	if (j1 < j2) {
		pthread_spin_unlock(&bcht->fg_locks[j2]);
		pthread_spin_unlock(&bcht->fg_locks[j1]);
	}
	else if (j1 > j2) {
		pthread_spin_unlock(&bcht->fg_locks[j1]);
		pthread_spin_unlock(&bcht->fg_locks[j2]);
	}
	else {
		pthread_spin_unlock(&bcht->fg_locks[j1]);
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

static inline
uint32_t cuckoo_read_even_count(bcht_t *bcht, size_t kidx)
{
	uint32_t c;

	do {
		c = BCHT_READ_KEYVER(bcht, kidx);
	} while (c & 1);

	return c;
}

static inline 
int32_t cuckoo_changed_count(bcht_t *bcht, size_t kidx, uint32_t c)
{
	return (c != BCHT_READ_KEYVER(bcht, kidx));
}

#if 0
static inline int32_t cuckoo_find_slot(uint32_t tags, tag_t tag)
{
	int32_t j;

	for (j = 0; j < BCHT_SLOT_SIZE; j++) {
		if (tag == ((tag_t *)&tags)[j]) {
			return j;
		}
	}

	return -1;
}

static inline 
void* cuckoo_find_data(bcht_t *bcht, tag_t tag, size_t idx, size_t kidx, const void *key, uint32_t nkey) 
{
	void *result = NULL, *data;
	uint32_t vs;
	volatile uint32_t tags;
	int32_t j;

	do {
		vs = cuckoo_read_even_count(bcht, kidx);
		//__builtin_prefetch(&(bcht->buckets[idx]));
		tags = *((uint32_t *)&(bcht->buckets[idx]));
		j = cuckoo_find_slot(tags, tag);

		if (cuckoo_changed_count(bcht, kidx, vs)) {
			continue;
		}
		else if (j < 0) {
			return NULL;
		}

		data = bcht->buckets[idx].slots[j];
		__builtin_prefetch(data);

		// call _compare_key()
		if (data && 
			bcht->cb_cmp_key((const void *)key, data, nkey) == 0) {
			result = data;
		}

	} while(cuckoo_changed_count(bcht, kidx, vs));

	return result;
}

#endif

/*
 * Try to read bucket i and check if the given tag is there
 */
static inline 
void* try_read(bcht_t *bcht, const char *key, const size_t nkey, tag_t tag, size_t bkidx, size_t kidx)
{
	uint32_t vs;
	volatile uint32_t tmp;
	int32_t sidx;
	void *result = NULL;

	do {
START:
		vs = cuckoo_read_even_count(bcht, kidx);
		__builtin_prefetch(&(bcht->buckets[bkidx]));
		tmp = *((uint32_t *)&(bcht->buckets[bkidx]));

		for (sidx = 0; sidx < BCHT_SLOT_SIZE; sidx++) {
			if (tag != ((uint8_t *)&tmp)[sidx]) {
				continue;
			}

			/* volatile __m128i p, q; */
			/* p = _mm_loadu_si128((__m128i const *) &buckets[i].slots[0]); */
			/* q = _mm_loadu_si128((__m128i const *) &buckets[i].slots[2]); */
			/* void *slots[4]; */

			/* _mm_storeu_si128((__m128i *) slots, p); */
			/* _mm_storeu_si128((__m128i *) (slots + 2), q); */
			/* void *data = slots[j]; */

			if (cuckoo_changed_count(bcht, kidx, vs)) {
				goto START;
			}

			result = NULL;
			void *data = bcht->buckets[bkidx].slots[sidx];
			__builtin_prefetch(data);

			if (data && bcht->cb_cmp_key((const void *)key, data, nkey) == 0) {
				result = data;
				break;
			}
		}

	} while (cuckoo_changed_count(bcht, kidx, vs));

	return result;
}

/*
 * Try to add an void to bucket i,
 * return true on success and false on failure
 */
static int32_t try_add(bcht_t *bcht, void *data, tag_t tag, size_t bkidx, size_t kidx)
{
	size_t j;

	for (j = 0; j < BCHT_SLOT_SIZE; j++) {
		if (BCHT_IS_SLOT_EMPTY(bcht, bkidx, j)) {
			// make the key odd
			BCHT_INC_KEYVER(bcht, kidx);

#ifdef BCHT_LOCK_FINEGRAIN
			fg_lock(bcht, bkidx, bkidx);
#endif

			bcht->buckets[bkidx].tags[j] = tag;
			bcht->buckets[bkidx].slots[j] = data;
			bcht->num_items++;

			// make the key even
			BCHT_INC_KEYVER(bcht, kidx);

#ifdef BCHT_LOCK_FINEGRAIN
			fg_unlock(bcht, bkidx, bkidx);
#endif
			return 1;
		}
	}

	return 0;
}

static void* try_del(bcht_t *bcht, const char *key, const size_t nkey, tag_t tag, size_t bkidx, size_t kidx)
{
	size_t j;
	void *data = NULL;

	for (j = 0; j < BCHT_SLOT_SIZE; j++) {
		if (!BCHT_IS_TAG_EQUAL(bcht, bkidx, j, tag)) {
			continue;
		}

		data = bcht->buckets[bkidx].slots[j];

		if (data == NULL) {
			// found but no data
			return NULL;
		}

		// call _compare_key()
		if (bcht->cb_cmp_key((const void *)key, data, nkey) == 0) {

#ifdef BCHT_LOCK_FINEGRAIN
			fg_lock(bcht, bkidx, bkidx);
#endif

			BCHT_INC_KEYVER(bcht, kidx);

			bcht->buckets[bkidx].tags[j] = 0;
			bcht->buckets[bkidx].slots[j] = 0;
			bcht->num_items--;

			BCHT_INC_KEYVER(bcht, kidx);

#ifdef BCHT_LOCK_FINEGRAIN
			fg_unlock(bcht, bkidx, bkidx);
#endif

			return data;
		}
	}

	return BCHT_RET_PTR_ERR;
}

/*
 * Make bucket  from[idx] slot[whichslot] available to insert a new void
 * return idx on success, -1 otherwise
 * @param from:   the array of bucket index
 * @param whichslot: the slot available
 * @param  depth: the current cuckoo depth
 */
static int32_t path_search(bcht_t *bcht, size_t depth_start, size_t *cp_index)
{
	int32_t depth = depth_start;

	while ((bcht->num_kick < BCHT_MAX_CUCKOO_COUNT) &&
		   (depth >= 0) && (depth < BCHT_MAX_CUCKOO_COUNT - 1)) {
		size_t *from = &(bcht->cuk_path[depth].cp_buckets[0]);
		size_t *to = &(bcht->cuk_path[depth + 1].cp_buckets[0]);

		/*
		 * Check if any slot is already free
		 */
		size_t idx;
		for (idx = 0; idx < BCHT_WIDTH; idx++) {
			size_t i = from[idx];
			size_t j;
			for (j = 0; j < BCHT_SLOT_SIZE; j++) {
				if (BCHT_IS_SLOT_EMPTY(bcht, i, j)) {
					bcht->cuk_path[depth].cp_slot_idxs[idx] = j;
					*cp_index = idx;

					return depth;
				}
			}

			// pick the victim item
			bcht->idx_victim++;
			j = bcht->idx_victim % BCHT_SLOT_SIZE;

			bcht->cuk_path[depth].cp_slot_idxs[idx] = j;
			bcht->cuk_path[depth].cp_slots[idx] = bcht->buckets[i].slots[j];
			to[idx] = alt_index(i, bcht->buckets[i].tags[j],
								bcht->tag_mask, bcht->hash_mask);
		}

		bcht->num_kick += BCHT_WIDTH;
		depth++;
	}

	return -1;
}

static int32_t move_backward(bcht_t *bcht, size_t depth_start, size_t idx)
{
	int32_t depth = depth_start;

	while (depth > 0) {
		size_t i1 = bcht->cuk_path[depth - 1].cp_buckets[idx];
		size_t i2 = bcht->cuk_path[depth].cp_buckets[idx];

		size_t j1 = bcht->cuk_path[depth - 1].cp_slot_idxs[idx];
		size_t j2 = bcht->cuk_path[depth].cp_slot_idxs[idx];

		/*
		 * We plan to kick out j1, but let's check if it is still there;
		 * there's a small chance we've gotten scooped by a later cuckoo.
		 * If that happened, just... try again.
		 */

		if (bcht->buckets[i1].slots[j1] !=
			bcht->cuk_path[depth - 1].cp_slots[idx]) {
			/* try again */
			return depth;
		}

		if (BCHT_IS_SLOT_EMPTY(bcht, i2, j2)) {
			size_t kidx   = keyver_index(i1, i2);
			BCHT_INC_KEYVER(bcht, kidx);

#ifdef BCHT_LOCK_FINEGRAIN
			fg_lock(bcht, i1, i2);
#endif

			bcht->buckets[i2].tags[j2] = bcht->buckets[i1].tags[j1];
			bcht->buckets[i2].slots[j2] = bcht->buckets[i1].slots[j1];

			bcht->buckets[i1].tags[j1] = 0;
			bcht->buckets[i1].slots[j1] = 0;

			bcht->num_moves++;

			BCHT_INC_KEYVER(bcht, kidx);

#ifdef BCHT_LOCK_FINEGRAIN
			fg_unlock(bcht, i1, i2);

#endif
			depth--;
		}
	}

	return depth;
}

static int32_t cuckoo(bcht_t *bcht, int32_t depth)
{
	int32_t cur;
	size_t idx;

	bcht->num_kick = 0;

	while (1) {
		cur = path_search(bcht, depth, &idx);
		if (cur < 0) {
			return -1;
		}

		cur = move_backward(bcht, cur, idx);
		if (cur == 0) {
			return idx;
		}

		depth = cur - 1;
	}

	return -1;
}

///////////////////////////////////////////////////////

bcht_t *
bcht_init_hash_table(const int32_t hashtable_init, bcht_cmp_key cmp_key)
{
	bcht_t *bcht;
	int32_t len = sizeof(bcht_t);

	bcht = cuckoo_malloc(len);
	if (bcht == NULL) {
		return NULL;
	}

	memset(bcht, 0, len);

	bcht->hash_power = BCHT_HASHPOWER_DEFAULT;
	if (hashtable_init) {
		bcht->hash_power = hashtable_init;
	}

	bcht->hash_size = (uint64_t)1 << (bcht->hash_power);
	bcht->hash_mask = bcht->hash_size - 1;

	/*
	 * tagpower: number of bits per tag
	 */
	bcht->tag_power = sizeof(tag_t) * 8;
	bcht->tag_mask = ((uint64_t)1 << bcht->tag_power) - 1;

	len = bcht->hash_size * sizeof(bcht_bucket_t);

	bcht->buckets = cuckoo_malloc(len);
	if (bcht->buckets == NULL) {
		goto FAIL;
	}

	memset(bcht->buckets, 0, len);

	len = sizeof(bcht_path_t) * BCHT_MAX_CUCKOO_COUNT;
	bcht->cuk_path = cuckoo_malloc(len);
	if (bcht->cuk_path == NULL) {
		goto FAIL;
	}

	memset(bcht->cuk_path, 0, len);

	len = sizeof(bcht_spinlock_t) * BCHT_LOCK_COUNT;
	bcht->fg_locks = cuckoo_malloc(len);
	if (bcht->fg_locks == NULL) {
		goto FAIL;
	}

	size_t i;
	for (i = 0; i < BCHT_LOCK_COUNT; i++) {
		pthread_spin_init(&bcht->fg_locks[i], PTHREAD_PROCESS_PRIVATE);
	}

	pthread_spin_init(&bcht->wlock, PTHREAD_PROCESS_PRIVATE);

    memset(bcht->keyver_array, 0, sizeof(uint32_t) * BCHT_KEYVER_COUNT);

	bcht->cb_cmp_key = dummy_cmp_key;
	if (cmp_key) {
		bcht->cb_cmp_key = cmp_key;
	}

	return bcht;

FAIL:
	if (bcht) {
		if (bcht->buckets) {
			cuckoo_free(bcht->buckets);
		}

		if (bcht->cuk_path) {
			cuckoo_free(bcht->cuk_path);
		}

		if (bcht->fg_locks) {
			cuckoo_free((void *)bcht->fg_locks);
		}

		cuckoo_free(bcht);
	}

	return NULL;
}

void bcht_destroy_hash_table(bcht_t *bcht)
{
	if (bcht->buckets) {
		cuckoo_free(bcht->buckets);
	}

	if (bcht->cuk_path) {
		cuckoo_free(bcht->cuk_path);
	}

	if (bcht->fg_locks) {
		cuckoo_free((void *)bcht->fg_locks);
	}

	cuckoo_free(bcht);
}

uint32_t bcht_hash(const char *key, const uint32_t len)
{
	uint32_t hash = 0;

	MurmurHash3_x86_32((const void*)key, (int32_t)len, (uint32_t)0x43606326, (void *)&hash);

	return hash;
}

void* bcht_find(bcht_t *bcht, const char *key, const size_t nkey, const uint32_t hash)
{
	tag_t tag;
	size_t i1, i2;
	void *result = NULL;
	size_t kidx;

	//hash = cuckoo_hash(key, nkey);
	tag = tag_hash(hash, bcht->tag_mask);
	i1 = index_hash(hash, bcht->hash_power);
	i2 = alt_index(i1, tag, bcht->tag_mask, bcht->hash_mask);
	kidx = keyver_index(i1, i2);

#ifdef BCHT_LOCK_FINEGRAIN
	fg_lock(bcht, i1, i2);
#endif

	result = try_read(bcht, key, nkey, tag, i1, kidx);
	if (result == NULL) {
		result = try_read(bcht, key, nkey, tag, i2, kidx);
	}

#ifdef BCHT_LOCK_FINEGRAIN
	fg_unlock(bcht, i1, i2);
#endif

	return result;
}

#if 0
/*
 * The interface to find a key in this hash table
 */
void* cuckoo_find1(bcht_t *bcht, const char *key, const size_t nkey, const uint32_t hash)
{
	tag_t tag;
	size_t i1, i2;

	//hash = cuckoo_hash(key, nkey);
	tag = tag_hash(hash, bcht->tag_mask);
	i1 = index_hash(hash, bcht->hash_power);
	i2 = alt_index(i1, tag, bcht->tag_mask, bcht->hash_mask);

	void *result = NULL;
	size_t kidx = keyver_index(i1, i2);
	uint32_t vs, ve;
	size_t j;
	volatile uint32_t tags1, tags2;

TRY_AGAIN:

#ifdef BCHT_LOCK_FINEGRAIN
	fg_lock(bcht, i1, i2);
#endif

	//vs = BCHT_READ_KEYVER(bcht, kidx);
	vs = cuckoo_read_even_count(bcht, kidx);
	tags1 = *((uint32_t *)&(bcht->buckets[i1]));

	//if (vs & 1) 
	//	goto TRY_AGAIN;

	for (j = 0; j < BCHT_SLOT_SIZE; j++) {
		uint8_t ch = ((uint8_t *)&tags1)[j];
		if (ch != tag) {
			continue;
		}

		ve = cuckoo_read_even_count(bcht, kidx);
		//ve = BCHT_READ_KEYVER(bcht, kidx);
		void *data = bcht->buckets[i1].slots[j];
		__builtin_prefetch(data);
		if (data == NULL) {
			continue;
		}

		//if ((ve & 1) || vs != ve)
		if (vs != ve)
			goto TRY_AGAIN;

		// call _compare_key()
		if (bcht->cb_cmp_key((const void *)key, data, nkey) == 0) {
			result = data;
			goto END;
		}
	}

	if (!result) {
		//__builtin_prefetch(&(bcht->buckets[i2]));
		tags2 = *((uint32_t *)&(bcht->buckets[i2]));

		for (j = 0; j < BCHT_SLOT_SIZE; j++) {
			uint8_t ch = ((uint8_t *)&tags2)[j];
			if (ch != tag) {
				continue;
			}

			//ve = BCHT_READ_KEYVER(bcht, kidx);
			ve = cuckoo_read_even_count(bcht, kidx);
			void *data = bcht->buckets[i2].slots[j];
			__builtin_prefetch(data);
			if (data == NULL) {
				continue;
			}

			//if (ve & 1 || vs != ve)
			if (vs != ve)
				goto TRY_AGAIN;

			// call _compare_key()
			if (bcht->cb_cmp_key((const void *)key, data, nkey) == 0) {
				result = data;
				break;
			}
		}
	}

END:

	//ve = BCHT_READ_KEYVER(bcht, kidx);
	ve = cuckoo_read_even_count(bcht, kidx);
	//if (ve & 1 || vs != ve)
	if (vs != ve)
		goto TRY_AGAIN;

#ifdef BCHT_LOCK_FINEGRAIN
	fg_unlock(bcht, i1, i2);
#endif

	return result;
}
#endif

// need to be protected by cache_lock
void* bcht_delete(bcht_t *bcht, const char *key, const size_t nkey, const uint32_t hash)
{
	tag_t tag;
	size_t i1, i2;
	void *data;

	//hash = cuckoo_hash(key, nkey);
	tag = tag_hash(hash, bcht->tag_mask);
	i1 = index_hash(hash, bcht->hash_power);
	i2 = alt_index(i1, tag, bcht->tag_mask, bcht->hash_mask);
	size_t kidx = keyver_index(i1, i2);

	pthread_spin_lock(&bcht->wlock);

	data = try_del(bcht, key, nkey, tag, i1, kidx);

	if (data == BCHT_RET_PTR_ERR) {
		data = try_del(bcht, key, nkey, tag, i2, kidx);
	}

	pthread_spin_unlock(&bcht->wlock);

	return data;
}

// need to be protected by cache_lock
int32_t bcht_insert(bcht_t *bcht, const uint32_t hash, void *data)
{
	tag_t tag;
	size_t i1, i2;
	int32_t ret = 0;

	//hash = cuckoo_hash(key, klen);
	tag = tag_hash(hash, bcht->tag_mask);
	i1 = index_hash(hash, bcht->hash_power);
	i2 = alt_index(i1, tag, bcht->tag_mask, bcht->hash_mask);
	size_t kidx = keyver_index(i1, i2);

	pthread_spin_lock(&bcht->wlock);

	if (try_add(bcht, data, tag, i1, kidx) ||
		try_add(bcht, data, tag, i2, kidx)) {
		ret = 1;
		goto END;
	}

	int32_t bkidx;
	size_t depth = 0;
	for (bkidx = 0; bkidx < BCHT_WIDTH; bkidx++) {
		if (bkidx < BCHT_WIDTH / 2) {
			bcht->cuk_path[depth].cp_buckets[bkidx] = i1;
		}
		else {
			bcht->cuk_path[depth].cp_buckets[bkidx] = i2;
		}
	}

	size_t j;
	bkidx = cuckoo(bcht, depth);
	if (bkidx >= 0) {
		i1 = bcht->cuk_path[depth].cp_buckets[bkidx];
		j = bcht->cuk_path[depth].cp_slot_idxs[bkidx];

		if (bcht->buckets[i1].slots[j] == 0 &&
			try_add(bcht, data, tag, i1, kidx)) {
			ret = 1;
			goto END;
		}
	}

	bcht->num_error++;

END:
	pthread_spin_unlock(&bcht->wlock);

	return ret;
}

void bcht_print_hashtable_info(bcht_t *bcht)
{
	size_t total_size = 0;

	printf("hash table is full (hashpower = %d, \
		num_items = %u, load factor = %.2f), \
		need to increase hashpower\n",
		   bcht->hash_power,
		   bcht->num_items,
		   1.0 * bcht->num_items / BCHT_SLOT_SIZE / bcht->hash_size);

	printf("num_items = %u\n", bcht->num_items);
	printf("index table size = %zu\n", bcht->hash_size);
	printf("hashtable size = %zu KB\n", 
		   bcht->hash_size * sizeof(bcht_bucket_t) / 1024);
	printf("hashtable load factor= %.5f\n", 
		   1.0 * bcht->num_items / BCHT_SLOT_SIZE / bcht->hash_size);
	total_size += bcht->hash_size * sizeof(bcht_bucket_t);
	printf("total_size = %zu KB\n", total_size / 1024);
	printf("moves per insert = %.2f\n", 
		   (double)bcht->num_moves / bcht->num_items);

	printf("\n");
}

/////////////////////////////////////////////////

static int bench_compare_key(const void *key1, const void *key2, const size_t nkey)
{
	const char *_key1 = (const char *)key1;
	//const char *_key2 = (const char *)key2;
	const char *_key2;
#if 0
	bcht_item_t *it = (bcht_item_t*)key2;
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

static bcht_item_t* bench_alloc_item(const char *key, const char *val, int len)
{
	bcht_item_t *it = cuckoo_malloc(sizeof(bcht_item_t));
	
	if (it == NULL) {
		return NULL;
	}

	it->key = strdup(key);
	it->key_len = len;
	it->value = strdup(val);

	return it;
}

static void fglock_free_item(bcht_item_t *it)
{
	if (it == NULL || it == BCHT_RET_PTR_ERR) {
		return;
	}

	cuckoo_free((void*)it->key);
	cuckoo_free(it->value);
	cuckoo_free(it);
}

static void* bench_init_hashtable(word_list_t *wordlist)
{
	bcht_t *ht;
	int i;

	///////////////////////////
	// init hash entries
	for (i = 0; i < wordlist->word_cnt; i++) {
		char *key = wordlist->word[i];
		char *val = key;

		bcht_item_t* it = bench_alloc_item(key, val, wordlist->lens[i]);
		wordlist->items[i] = (void*)it;
	}

	///////////////////////////
	// init hash tables
	ht = bcht_init_hash_table(23, bench_compare_key);

	printf("Hashtable size: %lu\n", ht->hash_size);
	fflush(NULL);

	return (void*)ht;
}

static void bench_print_hashtable_info(void *_ht, char *msg)
{
	bcht_t *ht = (bcht_t*)_ht;

	printf("%s key pairs: %d \n", msg, ht->num_items);
	printf("Cuckoo Movements: %d \n", ht->num_moves);
	fflush(NULL);
}

static int bench_insert_item(void *_ht, const char *key, const size_t len, void *val, uint32_t hash, void *data)
{
	bcht_t *ht = (bcht_t*)_ht;

	int ret;

	ret = bcht_insert(ht, hash, data);

	return ret;
}

static int bench_alloc_insert_data(void *_ht, const char *key, const size_t len, void *val, uint32_t hash)
{
	bcht_item_t* it = bench_alloc_item(key, val, len);
	
	if (it) {
		bench_insert_item(_ht,key, len, val, hash, it);
		return 0;
	}

	return -1;
}

static int bench_search_item(void *_ht, const char *key, const size_t len, uint32_t hash)
{
	bcht_t *ht = (bcht_t*)_ht;
	bcht_item_t *it;

	it = bcht_find(ht, key, len, hash);
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

static void* bench_delete_item(void *_ht, const char *key,  const size_t len, uint32_t hash)
{
	bcht_t *ht = (bcht_t*)_ht;
	bcht_item_t *it;

	it = bcht_delete(ht, key, len, hash);

	if (it != BCHT_RET_PTR_ERR && it != NULL) {
		fglock_free_item(it);
	}

	return it;
}

static void bench_clean_hashtable(void *_ht)
{
	bcht_t *ht = (bcht_t*)_ht;

	bcht_destroy_hash_table(ht);
}

//////////////////////////////////

cuckoo_bench_t fglock_bench = {
	.name = "Bucketized Cuckoo Hashtable",
	
	.bench_init_hash_table = bench_init_hashtable,
	.bench_clean_hash_table = bench_clean_hashtable,

	.bench_insert_item = bench_insert_item,
	.bench_alloc_insert_data = bench_alloc_insert_data,
	.bench_search_item = bench_search_item,
	.bench_delete_item = bench_delete_item,
	
	.bench_print_hash_table_info = bench_print_hashtable_info,
};
