#include <stdio.h>
#include <string.h>
#include <malloc.h>

#include "bin_cuckoo.h"
#if 0
#include "lib_cuckoo_hash.h"
#endif

#include "lf_hash.h"

typedef struct word_list_s {
	char ** word;
	int		word_cnt;
} word_list_t;

int get_words(word_list_t *w, int cnt)
{
	FILE *fp;
	char str[512];

	/* opening file for reading */
	fp = fopen("dictionary.txt", "r");

	if (fp == NULL) {
		perror("Error opening file");
		return -1;
	}

	int i;

	w->word = malloc(sizeof(char *) * cnt);

	for (i = 0; i < cnt; i++) {
		if (fgets(str, 512, fp) != NULL) {
			w->word[i] = strdup(str);
		}
		else {
			break;
		}
	}

	fclose(fp);

	w->word_cnt = i;

	return i;
}

int test_my_cuckoo(word_list_t *w, size_t elem_size)
{
	cuckoo_hashtable_t *ht;

	//size_t shift = 11;
	//size_t elem_size = POW2(shift);
	//printf("elem_size=%lu \n", elem_size);

	ht = cuckoo_alloc_hashtable(elem_size);
	if (ht == NULL) {
		printf("Cannot alloc hashtable ! \n");
		return -1;
	}

	int ret = 0, i, inserted;
	char *key;
	char *val;
	uint32_t klen;

	for (i = 0; i < w->word_cnt; i += 2) {
		key = strdup(w->word[i]);
		klen = strlen(key);
		val = strdup(w->word[i + 1]);

		ret = cuckoo_add_element(ht, (cuckoo_key_t)key, klen, (cuckoo_value_t)val);
		if (ret != 0) {
#if 0
			printf("%d: add ret=%d, used=%d, stash.used=%d \n",
				   i / 2, ret, ht->used, ht->stash.used);
#endif

			if (ret < 0) {
				break;
			}
		}
	}

	if (ret != 0) {
		inserted = i - 2;
	}
	else {
		inserted = i;
	}

	printf("adding key pairs: %d of %lu \n", inserted / 2, elem_size);
	printf("usage: %d of %u, stash.used:%d \n", ht->used, ht->element_size, ht->stash.used);
	printf("lookup key pairs: %d of %lu \n", inserted / 2, elem_size);

	fflush(NULL);

	for (i = 0; i < (inserted); i += 2) {
		key = strdup(w->word[i]);
		klen = strlen(key);
		val = w->word[i + 1];

		char *val1 = (char *)cuckoo_lookup_element(ht, (cuckoo_key_t)key, klen);

		if (val1 == NULL || strncmp(val, val1, klen) != 0) {
			printf("%d: mismatch value: val=%s, %s \n", i / 2, val, val1);
		}
	}

	printf("done. \n");


	return 0;
}

#if 1
int test_lib_cuckoo(word_list_t *w, size_t elem_size)
{
	struct cuckoo_hash ht;
	int power = 4;
	int max_depth = power << 5;
	int bin_size = 4;

	printf("hash size=%u, max_dep=%d \n", bin_size << 9, max_depth);

	if (!cuckoo_hash_init(&ht, 9)) {
		printf("Cannot alloc hashtable ! \n");
		return -1;
	}

	int i, inserted;
	char *key;
	char *val;
	uint32_t klen;

	for (i = 0; i < w->word_cnt; i += 2) {
		key = strdup(w->word[i]);
		klen = strlen(key);
		val = strdup(w->word[i + 1]);

		void *ret;

		ret = cuckoo_hash_insert(&ht, key, klen, val);
		if (ret != NULL || ret == CUCKOO_HASH_FAILED) {
			printf("insert failed: key=%s, val=%s \n", key, val);
#if 0
			printf("%d: add ret=%d, used=%d, stash.used=%d \n",
				   i / 2, ret, ht->used, ht->stash.used);
#endif
			break;
		}
	}

	printf("adding key pairs: %ld of %lu \n", ht.count, elem_size);
	printf("lookup key pairs: %ld of %lu \n", ht.count, elem_size);
	fflush(NULL);

	inserted = ht.count * 2;

	for (i = 0; i < (inserted); i += 2) {
		key = strdup(w->word[i]);
		klen = strlen(key);
		val = w->word[i + 1];

		char *val1 = NULL;
		struct cuckoo_hash_item *it;
		it = cuckoo_hash_lookup(&ht, key, klen);
		if (it) {
			val1 = (char *)it->value;
		}

		if (val1 == NULL || strncmp(val, val1, klen) != 0) {
			printf("%d: mismatch value: val=%s, %s \n", i / 2, val, val1);
		}
	}

	printf("done. \n");

	return 0;
}
#endif

int test_lf_cuckoo(word_list_t *w, size_t elem_size)
{
	int tid = 1;

	hash_table_t *ht = lfckht_init_hash_table(elem_size, 1);

	printf("hash size=%lu, max_dep=%d \n", elem_size, 1);
	fflush(NULL);

	if (!ht) {
		printf("Cannot alloc hashtable ! \n");
		return -1;
	}

	int i, inserted;
	char *key;
	char *val;
	uint32_t klen;

	for (i = 0; i < w->word_cnt; i += 2) {
		key = strdup(w->word[i]);
		klen = strlen(key);
		val = strdup(w->word[i + 1]);

		int ret;
		ret = lfckht_insert(ht, key, klen, val, tid);

		if (ret != 0) {
			printf("insert failed: ret=%d, key=%s, val=%s \n", ret, key, val);
#if 0
			printf("%d: add ret=%d, used=%d, stash.used=%d \n",
				   i / 2, ret, ht->used, ht->stash.used);
#endif
			if (ret < 0){
				break;
			}
		}
	}

	printf("adding key pairs: %d of %lu \n", ht->used, elem_size);
	printf("lookup key pairs: %d of %lu \n", ht->used, elem_size);
	fflush(NULL);

	inserted = ht->used * 2;
	int missed = 0;

	for (i = 0; i < (inserted); i += 2) {
		key = strdup(w->word[i]);
		klen = strlen(key);
		val = w->word[i + 1];

		char *val1 = NULL;
		int ret;

		ret = lfckht_search(ht, key, klen, tid, &val1);

		if (ret == 0 || val1 == NULL || strncmp(val, val1, klen) != 0) {
			//printf("%d: mismatch value: val=%s, %s \n", i / 2, val, val1);
			missed ++;
		}
	}

	printf("missed: %d of %u \n", missed, ht->used);
	printf("done. \n");

	return 0;
}

int main()
{
	size_t elem_size;
	word_list_t w;
	int cnt;

#define MAX_WORD 1000
	elem_size = 2000;

	cnt = get_words(&w, MAX_WORD);

	printf("word count:%d \n", cnt);

	//test_my_cuckoo(&w, elem_size);
	//test_lib_cuckoo(&w, elem_size);
	test_lf_cuckoo(&w, elem_size);


	return 0;
}
