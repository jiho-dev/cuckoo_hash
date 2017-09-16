
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <malloc.h>
#include <errno.h>

#include "cuckoo.h"
#include "cuckoo_malloc.h"
#include "MurmurHash3.h"

/////////////////////////////////////////////

#if 0
static uint64_t keycmp_mask[] = { 0x0000000000000000ULL,
								  0x00000000000000ffULL,
								  0x000000000000ffffULL,
								  0x0000000000ffffffULL,
								  0x00000000ffffffffULL,
								  0x000000ffffffffffULL,
								  0x0000ffffffffffffULL,
								  0x00ffffffffffffffULL };

uint16_t get_hash(void *ptr)
{
	return (uint16_t)(((uint64_t)ptr & 0xffff000000000000) >> 48);
}

void* store_hash(void *ptr, uint16_t hash)
{
	//uint64_t new_hash = hash;
	//new_hash = new_hash << 48;

	return (void *)(((uint64_t)hash << 48) +
					((uint64_t)ptr & 0x0000FFFFFFFFFFFF));
}

void* get_address(void *ptr)
{
	return (void *)((uint64_t)ptr & 0x0000fffffffffffc);
}

void reset_slot(cuckoo_slot_t *slot)
{
	slot->hash = 0;
	slot->data = NULL;
}

int32_t is_same_slot(cuckoo_slot_t *slot1, cuckoo_slot_t *slot2)
{
	if (slot1->hash == slot2->hash &&
		slot1->data == slot2->data) {
		return 1;
	}

	return 0;
}

static inline
int32_t keycmp(const char *key1, const char *key2, size_t len)
{
#define INT_KEYCMP_UNIT uint64_t

	INT_KEYCMP_UNIT v_key1;
	INT_KEYCMP_UNIT v_key2;
	size_t k = 0;

	while ((len) >= k + sizeof(INT_KEYCMP_UNIT)) {
		v_key1 = *(INT_KEYCMP_UNIT *)(key1 + k);
		v_key2 = *(INT_KEYCMP_UNIT *)(key2 + k);
		if (v_key1 != v_key2) {
			return 0;
		}

		k += sizeof(INT_KEYCMP_UNIT);
	}
	/*
	 * this code only works for little endian
	 */
	if (len - k) {
		v_key1 = *(INT_KEYCMP_UNIT *)(key1 + k);
		v_key2 = *(INT_KEYCMP_UNIT *)(key2 + k);

		return ((v_key1 ^ v_key2) & keycmp_mask[len - k]) == 0;
	}

	return 1;
}

static inline
size_t lock_index(const size_t i1, const size_t i2, const tag_t tag)
{
	/* if ((tag & locmask) == 0) { */
	/*     return i1; */
	/* } else { */
	/*     return i2; */
	/* } */
	//return tag;

	return i1 < i2 ? i1 : i2;
}

#endif

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

uint32_t cuckoo_hash(const char *key, const size_t klen)
{
	uint32_t hash = 0;

	MurmurHash3_x86_32(key, (int32_t)klen, 0x43606326, (void *)&hash);

	return hash;
}

/////////////////////////////////////////////////

static void fg_lock(cuckoo_hash_table_t *cukht, uint32_t i1, uint32_t i2)
{
	uint32_t j1, j2;

	j1 = i1 & FG_LOCK_MASK;
	j2 = i2 & FG_LOCK_MASK;

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
		for (idx = 0; idx < MEMC3_ASSOC_CUCKOO_WIDTH; idx++) {
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

		cukht->num_kick += MEMC3_ASSOC_CUCKOO_WIDTH;
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
			fg_lock(cukht, i1, i2);

			cukht->buckets[i2].tags[j2] = cukht->buckets[i1].tags[j1];
			cukht->buckets[i2].slots[j2] = cukht->buckets[i1].slots[j1];

			cukht->buckets[i1].tags[j1] = 0;
			cukht->buckets[i1].slots[j1] = 0;

			cukht->num_moves++;

			fg_unlock(cukht, i1, i2);

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

/*
 * Try to add an void to bucket i,
 * return true on success and false on failure
 */
static int32_t try_add(cuckoo_hash_table_t *cukht, void *data, tag_t tag, size_t i)
{
	size_t j;

	for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
		if (IS_SLOT_EMPTY(cukht, i, j)) {
			fg_lock(cukht, i, i);

			cukht->buckets[i].tags[j] = tag;
			cukht->buckets[i].slots[j] = data;
			cukht->num_items++;

			fg_unlock(cukht, i, i);

			return 1;
		}
	}

	return 0;
}

static void* try_del(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey, tag_t tag, size_t i)
{
	size_t j;

	void *data = NULL;

	for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
		if (!IS_TAG_EQUAL(cukht, i, j, tag)) {
			continue;
		}

		data = cukht->buckets[i].slots[j];

		if (data == NULL) {
			// found but no data
			return NULL;
		}

		if (cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {
			fg_lock(cukht, i, i);

			cukht->buckets[i].tags[j] = 0;
			cukht->buckets[i].slots[j] = 0;
			cukht->num_items--;

			fg_unlock(cukht, i, i);

			return data;
		}
	}

	return RET_PTR_ERR;
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

// need to be protected by cache_lock
void* cuckoo_delete(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey)
{
	tag_t tag;
	size_t i1, i2;
	uint32_t hv;
	void *data;

	hv = cuckoo_hash(key, nkey);
	tag = tag_hash(hv, cukht->tag_mask);
	i1 = index_hash(hv, cukht->hash_power);
	i2 = alt_index(i1, tag, cukht->tag_mask, cukht->hash_mask);

	data = try_del(cukht, key, nkey, tag, i1);
	if (data != RET_PTR_ERR) {
		return data;
	}

	data = try_del(cukht, key, nkey, tag, i2);
	if (data != RET_PTR_ERR) {
		return data;
	}

	return RET_PTR_ERR;
}

/*
 * The interface to find a key in this hash table
 */
void* cuckoo_find(cuckoo_hash_table_t *cukht, const char *key, const size_t nkey)
{
	uint32_t hv = 0;
	tag_t tag;
	size_t i1, i2;

	hv = cuckoo_hash(key, nkey);
	tag = tag_hash(hv, cukht->tag_mask);
	i1 = index_hash(hv, cukht->hash_power);
	i2 = alt_index(i1, tag, cukht->tag_mask, cukht->hash_mask);

	void *result = NULL;

	__builtin_prefetch(&(cukht->buckets[i1]));
	__builtin_prefetch(&(cukht->buckets[i2]));

	fg_lock(cukht, i1, i2);

#if 1
	volatile uint32_t tags1, tags2;

	tags1 = *((uint32_t *)&(cukht->buckets[i1]));
	tags2 = *((uint32_t *)&(cukht->buckets[i2]));

	size_t j;

	for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
		uint8_t ch = ((uint8_t *)&tags1)[j];
		if (ch != tag) {
			continue;
		}

		void *data = cukht->buckets[i1].slots[j];

		__builtin_prefetch(data);

		if (data == NULL) {
			continue;
		}

		// call _compare_key()
		if (cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {
			result = data;
			goto END;
		}
	}

	if (!result) {
		for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
			uint8_t ch = ((uint8_t *)&tags2)[j];
			if (ch != tag) {
				continue;
			}

			void *data = cukht->buckets[i2].slots[j];
			if (data == NULL) {
				continue;
			}

			if (cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {
				result = data;
				break;
			}
		}
	}
#else
	volatile uint32_t t[2] = {
		*((uint32_t *)&(cukht->buckets[i1])),
		*((uint32_t *)&(cukht->buckets[i2])),
	};

	size_t i, j;

	for (i=0; i<2; i++) {
		for (j = 0; j < BUCKET_SLOT_SIZE; j++) {
			uint8_t ch = ((uint8_t *)&t[i])[j];
			if (ch != tag) {
				continue;
			}

			void *data = cukht->buckets[i1].slots[j];
			if (data == NULL) {
				continue;
			}

			if (cukht->cb_cmp_key((const void *)key, data, nkey) == 0) {
				result = data;
				goto END;
				//break;
			}
		}
	}
#endif

END:

	fg_unlock(cukht, i1, i2);

	return result;
}

// need to be protected by cache_lock
int32_t cuckoo_insert(cuckoo_hash_table_t *cukht, const char *key, const size_t klen, void *data)
{
	tag_t tag;
	size_t i1, i2;
	uint32_t hv;

	hv = cuckoo_hash(key, klen);
	tag = tag_hash(hv, cukht->tag_mask);
	i1 = index_hash(hv, cukht->hash_power);
	i2 = alt_index(i1, tag, cukht->tag_mask, cukht->hash_mask);

	if (try_add(cukht, data, tag, i1)) {
		return 1;
	}

	if (try_add(cukht, data, tag, i2)) {
		return 1;
	}

	int32_t idx;
	size_t depth = 0;
	for (idx = 0; idx < MEMC3_ASSOC_CUCKOO_WIDTH; idx++) {
		if (idx < MEMC3_ASSOC_CUCKOO_WIDTH / 2) {
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
			try_add(cukht, data, tag, i1)) {
			return 1;
		}
	}

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
