#include <stdio.h>
#include <string.h>
#include <malloc.h>

#include "lf_hash.h"
#include "MurmurHash3.h"

//////////////////////////////////////////

void ht_remove(hash_table_t *ht, char *key, uint32_t klen, uint32_t tid);
void help_relocate(hash_table_t *ht, uint32_t which, uint32_t index, int initiator, uint32_t tid);
int find(hash_table_t *ht, char *key, uint32_t klen, count_ptr_t *_ptr1, count_ptr_t *_ptr2, uint32_t tid);
uint32_t hash1(hash_table_t *ht, char *key, uint32_t klen);
uint32_t hash2(hash_table_t *ht, char *key, uint32_t klen);
uint16_t check_counter(uint16_t ts1, uint16_t ts2, uint16_t ts1x, uint16_t ts2x);
void scan(hash_table_t *ht, uint32_t tid);


////////////////////////////////

count_ptr_t make_pointer(hash_entry_t *e, uint16_t count)
{
	return (count_ptr_t)((((uint64_t)count) << 48) | ((uint64_t)e & 0xFFFFFFFFFFFF));
}

hash_entry_t *get_pointer(count_ptr_t ptr)
{
	return (hash_entry_t *)((uint64_t)ptr & 0xFFFFFFFFFFFE);
}

uint16_t get_counter(count_ptr_t ptr)
{
	return (uint16_t)(((uint64_t)ptr >> 48) & 0xFFFF);
}

int get_marked(hash_entry_t *ent)
{
	return ((uint64_t)ent & 1) == 1;
}

hash_entry_t *set_marked(hash_entry_t *ent, int marked)
{
	return marked ? (hash_entry_t *)((uint64_t)ent | 1)
		   : (hash_entry_t *)((uint64_t)ent & (~1));
}

void rehash()
{
	return;
}

void retire_node(hash_table_t *ht, hash_entry_t *node, uint32_t tid)
{
	ht->rlist[tid][ht->rcount[tid]] = node;
	ht->rcount[tid]++;

	if (ht->rcount[tid] > MAX_DEPTH) {
		scan(ht, tid);
	}
}

void scan(hash_table_t *ht, uint32_t tid)
{
	// Stage 1
	uint32_t size = 0;

#if 0
	std::vector < hash_entry_t * > plist;

	for (uint32_t i = 0; i < ht->hp_rec.size(); i++) {
		for (uint32_t j = 0; j < ht->hp_rec[i].size(); j++) {
			hash_entry_t *hptr = ht->hp_rec[i][j];
			if (hptr != NULL) {
				plist.push_back(hptr);
			}
		}
	}

	// Stage 2
	uint32_t n = ht->rcount[tid];
	ht->rcount[tid] = 0;

	for (uint32_t i = 0; i < n; i++) {
		if (std::find(plist.begin(), plist.end(), ht->rlist[tid][i]) != plist.end()) {
			ht->rlist[tid][ht->rcount[tid]] = ht->rlist[tid][i];
			ht->rcount[tid]++;
		}
		else {
			//printf("freed %p\n", rlist[tid][i]);
			delete ht->rlist[tid][i];
		}
	}
#endif

}

int relocate(hash_table_t *ht, uint32_t which, uint32_t index, uint32_t tid)
{
	int route[THRESHOLD];
	count_ptr_t pptr;
	int pre_idx;
	int start_level;
	int tbl;
	uint32_t idx;

	int found;
	int depth;

try_again:
	pptr = NULL;
	pre_idx = 0;
	start_level = 0;
	tbl = which;
	idx = index;
	found = 0;
	depth = start_level;

path_discovery:
	do {
		count_ptr_t ptr1 = ht->table[tbl][idx];

		while (get_marked(ptr1)) {
			help_relocate(ht, tbl, idx, 0, tid);
			ptr1 = ht->table[tbl][idx];
		}

		hash_entry_t *e1 = get_pointer(ptr1);
		hash_entry_t *p1 = get_pointer(pptr);
		ht->hp_rec[tid][0] = e1;
		if (e1 != get_pointer(ht->table[tbl][idx])) {
			goto try_again;
		}
		/*
		 * if (p1 && e1 && e1->key == p1->key) {
		 * if (tbl == 0)
		 * del_dup(idx, ptr1, pre_idx, pptr, tid);
		 * else
		 * del_dup(pre_idx, pptr, idx, ptr1, tid);
		 * }
		 */

		if (e1 != NULL) {
			route[depth] = idx;
			char *key = e1->key;
			uint32_t klen = e1->klen;
			pptr = ptr1;
			pre_idx = idx;
			tbl = 1 - tbl;
			idx = (tbl == 0) ? hash1(ht, key, klen) : hash2(ht, key, klen);
		}
		else {
			found = 1;
		}
	} while (!found && ++depth < THRESHOLD);

	if (!found) {
		return 0;
	}

	tbl = 1 - tbl;
	for (int i = depth - 1; i >= 0; i--, tbl = 1 - tbl) {
		idx = route[i];
		count_ptr_t ptr1 = ht->table[tbl][idx];
		/*
		 * hp_rec[tid][0] = get_pointer(ptr1);
		 * if (get_pointer(ptr1) != get_pointer(table[tbl][idx]))
		 * goto try_again;
		 */
		if (get_marked(ptr1)) {
			help_relocate(ht, tbl, idx, 0, tid);
			ht->hp_rec[tid][0] = ht->table[tbl][idx];
			ptr1 = ht->hp_rec[tid][0];
			/*
			 * if (get_pointer(ptr1) != get_pointer(table[tbl][idx]))
			 * goto try_again;
			 */
		}

		hash_entry_t *e1 = get_pointer(ptr1);
		if (e1 == NULL) {
			continue;
		}

		uint32_t dest_idx = (tbl == 0) ? 
			hash2(ht, e1->key, e1->klen) : 
			hash1(ht, e1->key, e1->klen);

		count_ptr_t ptr2 = ht->table[1 - tbl][dest_idx];
		hash_entry_t *e2 = get_pointer(ptr2);

		if (e2 != NULL) {
			start_level = i + 1;
			idx = dest_idx;
			tbl = 1 - tbl;
			goto path_discovery;
		}

		help_relocate(ht, tbl, idx, 1, tid);
	}

	return found;
}

void del_dup(hash_table_t *ht, uint32_t idx1, count_ptr_t ptr1, uint32_t idx2, count_ptr_t ptr2, uint32_t tid)
{
	ht->hp_rec[tid][0] = ptr1;
	ht->hp_rec[tid][1] = ptr2;

	if (ptr1 != ht->table[0][idx1] && ptr2 != ht->table[1][idx2]) {
		return;
	}

	if (get_pointer(ptr1)->key != get_pointer(ptr2)->key) {
		return;
	}

	reset_pointer(&ht->table[1][idx2], ptr2);
}

int lfckht_search(hash_table_t *ht, char *key, uint32_t klen, uint32_t tid, char **val)
{
	uint32_t h1 = hash1(ht, key, klen);
	uint32_t h2 = hash2(ht, key, klen);
	
	while (1) {
		count_ptr_t ptr1 = ht->table[0][h1];
		hash_entry_t *e1 = get_pointer(ptr1);

		ht->hp_rec[tid][0] = e1;
		if (ptr1 != ht->table[0][h1]) {
			continue;
		}

		uint16_t ts1 = get_counter(ptr1);

		if (e1 && strncmp(e1->key, key, klen) == 0) {
			*val = e1->val;
			return 1;
		}

		count_ptr_t ptr2 = ht->table[1][h2];
		hash_entry_t *e2 = get_pointer(ptr2);

		ht->hp_rec[tid][0] = e2;
		if (ptr2 != ht->table[1][h2]) {
			continue;
		}

		uint16_t ts2 = get_counter(ptr2);

		if (e2 && strncmp(e2->key, key, klen) == 0) {
			*val = e2->val;
			return 1;
		}

		uint16_t ts1x = get_counter(ht->table[0][h1]);
		uint16_t ts2x = get_counter(ht->table[1][h2]);

		if (check_counter(ts1, ts2, ts1x, ts2x)) {
			continue;
		}
		else {
			return 0;
		}
	}

	return 0;
}

int lfckht_insert(hash_table_t *ht, char *key, uint32_t klen, char *val, uint32_t tid)
{
	count_ptr_t ptr1, ptr2;

	hash_entry_t *new_node = (hash_entry_t *)malloc(sizeof(hash_entry_t));

	new_node->key = key;
	new_node->klen = klen;
	new_node->val = val;

	uint32_t h1 = hash1(ht, key, klen);
	uint32_t h2 = hash2(ht, key, klen);

	while (1) {
		int result = find(ht, key, klen, &ptr1, &ptr2, tid);

		if (result == FIRST) {
			hash_entry_t *e = get_pointer(ptr1);

			if (e->key) {
				free(e->key);
			}

			e->val = val;

			return 1;
		}

		if (result == SECOND) {
			hash_entry_t *e = get_pointer(ptr2);

			if (e->key) {
				free(e->key);
			}

			e->val = val;

			return 1;
		}

		if (!get_pointer(ptr1)) {
			if (!set_pointer(&ht->table[0][h1], ptr1, new_node)) {
				continue;
			}

			ht->used ++;
			return 0;
		}

		if (!get_pointer(ptr2)) {
			if (!set_pointer(&ht->table[1][h2], ptr2, new_node)) {
				continue;
			}

			ht->used ++;
			return 0;
		}

		if (relocate(ht, 0, h1, tid)) {
			continue;
		}
		else {
			rehash();
			return -1;
		}
	}

	return -2;
}

hash_table_t *lfckht_init_hash_table(uint32_t capacity, uint32_t thread_count)
{
	hash_table_t *ht;

	ht = (hash_table_t *)malloc(sizeof(hash_table_t));
	if (ht == NULL) {
		return NULL;
	}

	memset(ht, 0, sizeof(hash_table_t));

	ht->size1 = capacity / 2;
	ht->size2 = capacity - ht->size1;

	ht->table[0] = (count_ptr_t *)malloc(sizeof(count_ptr_t) * ht->size1);
	ht->table[1] = (count_ptr_t *)malloc(sizeof(count_ptr_t) * ht->size2);

	//hp_rec.reserve(thread_count);
	//rlist.reserve(thread_count);
	//rcount.reserve(thread_count);

	for (int i = 0; i < thread_count; i++) {
		ht->hp_rec[i][0] = NULL;
		ht->hp_rec[i][1] = NULL;
		ht->rcount[i] = 0;
	}

	return ht;
}

void clean_hash_table(hash_table_t *ht)
{
	if (ht->table[0]) {
		free(ht->table[0]);
	}

	if (ht->table[1]) {
		free(ht->table[1]);
	}

	free(ht);
}

uint32_t hash1(hash_table_t *ht, char *key, uint32_t klen)
{
	uint32_t h1;

	MurmurHash3_x86_32((const void *)key, klen, 0xddee1234, (void *)&h1);

	return (h1 % ht->size1);

#if 0
	int c2 = 0x27d4eb2d; // a prime or an odd constant

	key = (key ^ 61) ^ (key >> 16);
	key = key + (key << 3);
	key = key ^ (key >> 4);
	key = key * c2;
	key = key ^ (key >> 15);

	return key % ht->size1;
#endif

}

uint32_t hash2(hash_table_t *ht, char *key, uint32_t klen)
{
	uint32_t h1;

	MurmurHash3_x86_32((const void *)key, klen, 0xaabbccdd, (void *)&h1);

	return (h1 % ht->size1);
#if 0
	key = ((key >> 16) ^ key) * 0x45d9f3b;
	key = ((key >> 16) ^ key) * 0x45d9f3b;
	key = (key >> 16) ^ key;

	return key % ht->size2;
#endif
}

uint16_t check_counter(uint16_t ts1, uint16_t ts2, uint16_t ts1x, uint16_t ts2x)
{
	return (ts1x >= ts1 + 2) && (ts2x >= ts2 + 2) && (ts2x >= ts1 + 3);
}

int find(hash_table_t *ht, char *key, uint32_t klen, count_ptr_t *_ptr1, count_ptr_t *_ptr2, uint32_t tid)
{
	uint32_t h1 = hash1(ht, key, klen);
	uint32_t h2 = hash2(ht, key, klen);
	
	int result = NIL;

	if (h1 == h2) {
		printf("h1=0x%x, h2=0x%x \n", h1, h2);
	}

	count_ptr_t ptr1, ptr2;

	while (1) {
		ptr1 = ht->table[0][h1];
		uint16_t ts1 = get_counter(ptr1);

		ht->hp_rec[tid][0] = get_pointer(ptr1);

		if (get_pointer(ptr1) != get_pointer(ht->table[0][h1])) {
			continue;
		}

		if (get_pointer(ptr1)) {
			if (get_marked(ptr1)) {
				help_relocate(ht, 0, h1, 0, tid);
				continue;
			}

			char *p = get_pointer(ptr1)->key;

			if (strncmp(p, key, klen) == 0) {
				result = FIRST;
			}
		}

		ptr2 = ht->table[1][h2];
		uint16_t ts2 = get_counter(ptr2);

		ht->hp_rec[tid][1] = get_pointer(ptr2);
		if (get_pointer(ptr2) != get_pointer(ht->table[1][h2])) {
			continue;
		}

		if (get_pointer(ptr2)) {
			if (get_marked(ptr2)) {
				help_relocate(ht, 1, h2, 0, tid);
				continue;
			}

			char *p = get_pointer(ptr2)->key;

			if (strncmp(p, key, klen) == 0) {
				if (result == FIRST) {
					del_dup(ht, h1, ptr1, h2, ptr2, tid);
				}
				else {
					result = SECOND;
				}
			}
		}

		if (result == FIRST || result == SECOND) {
			*_ptr1 = ptr1;
			*_ptr2 = ptr2;
			return result;
		}

		ptr1 = ht->table[0][h1];
		ptr2 = ht->table[1][h2];

		if (check_counter(ts1, ts2, get_counter(ptr1), get_counter(ptr2))) {
			continue;
		}
		else {
			*_ptr1 = ptr1;
			*_ptr2 = ptr2;
			return NIL;
		}
	}

	*_ptr1 = ptr1;
	*_ptr2 = ptr2;

	return NIL;
}

void help_relocate(hash_table_t *ht, uint32_t which, uint32_t index, int initiator, uint32_t tid)
{
	while (1) {
		count_ptr_t ptr1 = ht->table[which][index];
		hash_entry_t *src = get_pointer(ptr1);
		ht->hp_rec[tid][0] = src;

		if (ptr1 != ht->table[which][index]) {
			continue;
		}

		while (initiator && !get_marked(ptr1)) {
			if (src == NULL) {
				return;
			}

			set_mark(&ht->table[which][index], ptr1);

			ptr1 = ht->table[which][index];
			ht->hp_rec[tid][0] = ptr1;
			if (ptr1 != ht->table[which][index]) {
				continue;
			}

			src = get_pointer(ptr1);
		}

		if (!get_marked(ptr1)) {
			return;
		}

		uint32_t hd = ((1 - which) == 0) ? 
			hash1(ht, src->key, src->klen) : 
			hash2(ht, src->key, src->klen);

		count_ptr_t ptr2 = ht->table[1 - which][hd];
		hash_entry_t *dst = get_pointer(ptr2);
		ht->hp_rec[tid][1] = dst;
		if (ptr2 != ht->table[1 - which][hd]) {
			continue;
		}

		uint16_t ts1 = get_counter(ptr1);
		uint16_t ts2 = get_counter(ptr2);

		if (dst == NULL) {
			uint16_t nCnt = ts1 > ts2 ? ts1 + 1 : ts2 + 1;

			if (ptr1 != ht->table[which][index]) {
				continue;
			}

			if (set_pointer_count(&ht->table[1 - which][hd], 
								  ptr2, src, nCnt)) {
				set_pointer_count(&ht->table[which][index], ptr1, 
								  NULL, ts1 + 1);
				return;
			}
		}

		if (src == dst) {
			set_pointer_count(&ht->table[which][index], ptr1, 
							  NULL, ts1 + 1);
			return;
		}

		set_pointer_count(&ht->table[which][index], ptr1, 
						  set_marked(src, 0), ts1 + 1);
		return;
	}
}

void ht_remove(hash_table_t *ht, char *key, uint32_t klen, uint32_t tid)
{
	uint32_t h1 = hash1(ht, key, klen);
	uint32_t h2 = hash2(ht, key, klen);
	
	count_ptr_t e1;
	count_ptr_t e2;

	while (1) {
		int ret = find(ht, key, klen, &e1, &e2, tid);

		if (ret == NIL) {
			return;
		}

		if (ret == FIRST) {
			if (reset_pointer(&ht->table[0][h1], e1)) {
				retire_node(ht, get_pointer(e1), tid);
				return;
			}
		}
		else if (ret == SECOND) {
			if (ht->table[0][h1] != e1) {
				continue;
			}

			if (reset_pointer(&ht->table[1][h2], e2)) {
				retire_node(ht, get_pointer(e2), tid);
				return;
			}
		}
	}
}

//////////////////////////////////////////////

#if 0
hash_entry_t test_vec[] =  {
	{1,1},
	{1,1},
	{2,2},
	{3,3},
	{4,5},

};

int main()
{
	hash_table_t *ht = init_hash_table(2048, 1);
	int i, tid = 100;
	int s = sizeof(test_vec) / sizeof(hash_entry_t);

	for (i=0; i<s; i++) {
		hash_entry_t *t = &test_vec[i];
		insert(ht, t->key, t->val, tid);
	}

	int val = 0;
	int ret, missed=0;;

	for (i=0; i<s; i++) {
		hash_entry_t *t = &test_vec[i];

		ret = search(ht, t->key, tid, &val);

		if (ret == 0 || val != t->val) {
			printf("Not found: ret=%d, key=%d, val=%d \n", ret, t->key, t->val);
			missed ++;
		}
	}

	printf("\n=== Test Result===\n");
	printf("Node:\t%d\n", s);
	printf("Missed:\t%d\n", missed);


#if 0
	__int128_t x = 0;
	__sync_bool_compare_and_swap(&x,0,10);
#endif

	return 0;
}

#endif
