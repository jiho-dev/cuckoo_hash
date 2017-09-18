#include <stdio.h>
#include <string.h>

#include "MurmurHash3.h"
#include "cuckoo_malloc.h"
#include "cuckoo.h"



cuckoo_hashtable_t *
cuckoo_alloc_hashtable(size_t elem_size)
{
	//size_t elem_size  = POW2(shift);
	size_t len;
	cuckoo_elem_t *elems = NULL;
	cuckoo_hashtable_t *ht = NULL;
	uint16_t bin_size = CUCKOO_BIN_SIZE;

	elem_size = (elem_size / bin_size) * bin_size;

	// alloc elements
	len = sizeof(cuckoo_elem_t) * elem_size;
	elems = cuckoo_malloc(len);
	if (elems == NULL) {
		goto FAIL;
	}

	memset(elems, 0, len);

	// alloc hashtable
	len = sizeof(cuckoo_hashtable_t);
	ht = cuckoo_malloc(len);
	if (ht == NULL) {
		goto FAIL;
	}

	memset(ht, 0, len);

	// initialize members
	ht->elements = elems;
	ht->element_size = elem_size;
	ht->bucket_size = elem_size / bin_size;
	ht->bin_size = bin_size;
	ht->max_loops = CUCKOO_MAX_LOOP;

	return ht;

FAIL:
	if (elems) {
		cuckoo_free(elems);
	}

	if (ht) {
		cuckoo_free(ht);
	}

	return NULL;
}

void
cuckoo_free_hashtable(cuckoo_hashtable_t *ht)
{
	if (ht == NULL) {
		return;
	}

	if (ht->elements) {
		cuckoo_free(ht->elements);
	}

	cuckoo_free(ht);
}

static cuckoo_hashkey_t
cuckoo_get_hashkey(cuckoo_key_t key, uint32_t klen)
{
	union {
		uint32_t			result[4];
		cuckoo_hashkey_t	hashes;
	} u;

	const unsigned char *const data = (const unsigned char *)key;

	MurmurHash3_x86_128(data, (const int)klen, 0xB3ECE62A, &u.result);

#if (CUCKOO_HASH_SIZE == 32)
	u.result[0] ^= u.result[2];
	u.result[1] ^= u.result[3];
#endif

	// do not keep identical hashes
	if (u.hashes.hash[0] == u.hashes.hash[1]) {
		u.hashes.hash[1] = ~u.hashes.hash[1];
	}

	return u.hashes;
}

static uint32_t
cuckoo_get_position(cuckoo_hashtable_t *ht, cuckoo_hashvalue_t hval)
{
	uint32_t pos;

#if 1
	pos = (uint32_t)((hval % ht->bucket_size) * ht->bin_size);
#else
	pos = (uint32_t)(hval & (ht->element_size - 1));
#endif

	return pos;
}

static cuckoo_elem_t *
cuckoo_get_element(cuckoo_hashtable_t *ht, cuckoo_hashvalue_t hval)
{
	uint32_t pos = cuckoo_get_position(ht, hval);

	return &ht->elements[pos];
}

static cuckoo_elem_t *
cuckoo_get_stash_element(cuckoo_hashtable_t *ht, uint32_t pos)
{
	return &ht->stash.elements[pos % CUCKOO_STASH_SIZE];
}

static int
cuckoo_compare_element(cuckoo_elem_t *elem, const cuckoo_hashkey_t *hk, const cuckoo_key_t key, uint32_t klen)
{
	if (elem->hashkey.hash[0] == hk->hash[0] &&
		elem->hashkey.hash[1] == hk->hash[1] &&
		elem->keylen == klen &&
		memcmp(elem->key, key, klen) == 0) {
		return 1;
	}

	return 0;
}

static void
cuckoo_set_element(cuckoo_elem_t *elem, const cuckoo_hashkey_t *hk, const cuckoo_key_t key, uint32_t klen, cuckoo_value_t value)
{
	elem->hashkey = *hk;
	elem->key = key;
	elem->keylen = klen;
	elem->value = value;
}

static int
cuckoo_element_is_free(const cuckoo_hashtable_t *ht, uint32_t pos)
{
	return ht->elements[pos].keylen == 0;
}

static int
cuckoo_insert_stash(cuckoo_hashtable_t *ht, cuckoo_elem_t elem)
{
	if (ht->stash.used >= CUCKOO_STASH_SIZE) {
		return -1;
	}

	ht->stash.elements[ht->stash.used] = elem;
	ht->stash.used++;

	return 0;
}

static int
cuckoo_insert_element(cuckoo_hashtable_t *ht, cuckoo_elem_t elem)
{
	cuckoo_hashvalue_t hv, init_hv;
	uint32_t i, j;
	uint32_t pos, init_pos;

	// is there any free element ?
	for (i = 0; i < CUCKOO_NUM_HASH; i++) {
		pos = cuckoo_get_position(ht, elem.hashkey.hash[i]);
		for (j = 0; j < ht->bin_size; j++) {
			uint32_t next_pos = pos + j;

			if (cuckoo_element_is_free(ht, next_pos)) {
				ht->used++;
				ht->elements[next_pos] = elem;
				// added
				return 0;
			}
		}
	}

	if (ht->stash.used >= CUCKOO_STASH_SIZE) {
		printf("stash is full. Rehashing needs immediately \n");
		return -5;
	}

	// no, it isn't. now, do cuckoo at the first hash
	hv = init_hv = elem.hashkey.hash[0];
	init_pos = cuckoo_get_position(ht, init_hv);

	//printf("now start to cuckoo to insert an element: hashvalue=0x%x \n", (uint32_t)hv);

	for (i = 0; i < ht->max_loops; i++) {
		pos = cuckoo_get_position(ht, hv);

		// place it at alternate free position
		for (j = 0; j < ht->bin_size; j++) {
			uint32_t next_pos = pos + j;

			if (cuckoo_element_is_free(ht, next_pos)) {
				ht->elements[next_pos] = elem;
				ht->used++;
				// cuckooed
				return 2;
			}

			// cuckooing
			const cuckoo_elem_t cuckooed_elem = ht->elements[next_pos];

			ht->elements[next_pos] = elem;
			elem = cuckooed_elem;
		}

		// cuckooed_elem was placed into its first position
		if (pos == cuckoo_get_position(ht, elem.hashkey.hash[0])) {
			hv = elem.hashkey.hash[1];
		}
		// cuckooed_elem was placed into its second position
		else if (pos == cuckoo_get_position(ht, elem.hashkey.hash[1])) {
			hv = elem.hashkey.hash[0];
		}
		else {
			printf("We don't where cuckooed elem came from");
		}

		if (hv == init_hv && cuckoo_get_position(ht, hv) == init_pos) {
			printf("loopcnt=%d \n", i);
			// no more free position
			// need rehash, but put it off sometime by keeping it at stash
			break;
		}
	}

	if (cuckoo_insert_stash(ht, elem) < 0) {
		printf("stash is full. Rehashing needs immediately \n");
		return -1;
	}

	// inserted stash
	return 3;
}

int
cuckoo_add_element(cuckoo_hashtable_t *ht, cuckoo_key_t key, uint32_t klen, cuckoo_value_t value)
{
	const cuckoo_hashkey_t hk = cuckoo_get_hashkey(key, klen);
	cuckoo_elem_t *elem;
	int ret;
	int i, j;

	for (i = 0; i < CUCKOO_NUM_HASH; i++) {
		elem = cuckoo_get_element(ht, hk.hash[i]);

		for (j = 0; j < ht->bin_size; j++, elem++) {
			ret = cuckoo_compare_element(elem, &hk, (const cuckoo_key_t)key, klen);
			if (ret) {
				cuckoo_set_element(elem, &hk, key, klen, value);
				// replaced
				return 1;
			}
		}
	}

	if (ht->stash.used > 0) {
		for (i = 0; i < CUCKOO_STASH_SIZE; i++) {
			elem = &ht->stash.elements[i];
			ret = cuckoo_compare_element(elem, &hk, (const cuckoo_key_t)key, klen);
			if (ret) {
				cuckoo_set_element(elem, &hk, key, klen, value);
				// replaced into stash
				return 1;
			}
		}
	}

	cuckoo_elem_t new_elem;

	new_elem.hashkey = hk;
	new_elem.key = key;
	new_elem.keylen = klen;
	new_elem.value = value;

	return cuckoo_insert_element(ht, new_elem);
}

static void
cuckoo_reset_element(cuckoo_elem_t *elem)
{
	elem->key = NULL;
	elem->keylen = 0;
	elem->value = NULL;
	memset(&elem->hashkey, 0, sizeof(cuckoo_hashkey_t));
}

static int
cuckoo_remove_element(cuckoo_hashtable_t *ht, cuckoo_key_t key, uint32_t klen, uint32_t *pos)
{
	const cuckoo_hashkey_t hk = cuckoo_get_hashkey(key, klen);
	cuckoo_elem_t *elem;
	int ret;
	int i, j;

	for (i = 0; i < CUCKOO_NUM_HASH; i++) {
		elem = cuckoo_get_element(ht, hk.hash[i]);

		for (j = 0; j < ht->bin_size; j++, elem++) {
			ret = cuckoo_compare_element(elem, &hk, (const cuckoo_key_t)key, klen);
			if (ret) {
				cuckoo_reset_element(elem);

				// removed it at elements
				*pos = cuckoo_get_position(ht, hk.hash[i]);
				return j;
			}
		}
	}

	if (ht->stash.used == 0) {
		// not found
		return -1;
	}

	for (i = 0; i < CUCKOO_STASH_SIZE; i++) {
		elem = &ht->stash.elements[i];
		ret = cuckoo_compare_element(elem, &hk, (const cuckoo_key_t)key, klen);
		if (ret) {
			ht->stash.used--;

			cuckoo_reset_element(elem);

			// removed it at stash
			return -2;
		}
	}

	// not found
	return -1;
}

int
cuckoo_delete_element(cuckoo_hashtable_t *ht, cuckoo_key_t key, uint32_t klen)
{
	uint32_t pos;
	int deleted_offset;
	int i;

	deleted_offset = cuckoo_remove_element(ht, key, klen, &pos);
	if (deleted_offset < 0) {
		return -1;
	}

	// try to move an element from stash to elements
	for (i = 0; i < CUCKOO_STASH_SIZE; i++) {
		cuckoo_elem_t *elem = &ht->stash.elements[i];

		const uint32_t pos1 = cuckoo_get_position(ht, elem->hashkey.hash[0]);
		const uint32_t pos2 = cuckoo_get_position(ht, elem->hashkey.hash[1]);

		if (pos != pos1 && pos != pos2) {
			continue;
		}

		ht->elements[pos + deleted_offset] = *elem;

		// move the rest of elems forward
		for (; (i + 1) < CUCKOO_STASH_SIZE; i++) {
			ht->stash.elements[i] = ht->stash.elements[i + 1];
		}

		ht->stash.used--;
		break;
	}

	return 0;
}

cuckoo_value_t
cuckoo_lookup_element(cuckoo_hashtable_t *ht, cuckoo_key_t key, uint32_t klen)
{
	const cuckoo_hashkey_t hk = cuckoo_get_hashkey(key, klen);
	cuckoo_elem_t *elem;
	int ret;
	int i, j;

	for (i = 0; i < CUCKOO_NUM_HASH; i++) {
		elem = cuckoo_get_element(ht, hk.hash[i]);

		for (j = 0; j < ht->bin_size; j++, elem++) {
			ret = cuckoo_compare_element(elem, &hk, (const cuckoo_key_t)key, klen);
			if (ret) {
				return elem->value;
			}
		}
	}

	if (ht->stash.used == 0) {
		return NULL;
	}

	for (i = 0; i < CUCKOO_STASH_SIZE; i++) {
		elem = &ht->stash.elements[i];
		ret = cuckoo_compare_element(elem, &hk, (const cuckoo_key_t)key, klen);
		if (ret) {
			return elem->value;
		}
	}

	return (cuckoo_value_t)NULL;
}
