#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>

#include "cuckoo.h"
#include "cuckoo_malloc.h"

typedef struct word_list_s {
	char	**word;
	int 	*lens;
	uint32_t *hash;

	int		alloc_cnt;
	int		word_cnt;
} word_list_t;

pthread_t threads[5];
int done[5];

typedef struct thread_arg_s {
	int idx;
	int act;
	int nwait;
	int round;

	word_list_t *w;
	cuckoo_hash_table_t *cukht;
} thread_arg_t;

typedef void *(*start_routine) (void *);

void *thread_main_delete(void *arg);
void *thread_main_reinsert(void *arg);
int thread_main(word_list_t *w, cuckoo_hash_table_t *cukht);

/////////////////////////////////



int get_line_count(FILE *fp)
{
	int lines = 0;
	int ch;
	int charsOnCurrentLine = 0;

	while ((ch = fgetc(fp)) != EOF) {
		if (ch == '\n') {
			lines++;
			charsOnCurrentLine = 0;
		}
		else {
			charsOnCurrentLine++;
		}
	}
	if (charsOnCurrentLine > 0) {
		lines++;
	}

	return lines;
}

void get_words(word_list_t *w, int cnt)
{
	FILE *fp;
	char str[512];
	char *filename[] = {
		"wordlist1.txt",
		"wordlist2.txt",
		"wordlist3.txt",
		NULL
	};

	int idx = 0;

	if (cnt < 1) {
		while (1) {
			char *p = filename[idx];

			if (p == NULL) {
				break;
			}

			fp = fopen(p, "r");

			if (fp == NULL) {
				perror("Error opening file");
				break;
			}

			cnt += get_line_count(fp);
			fclose(fp);
			idx++;
		}
	}

	w->word_cnt = 0;
	w->alloc_cnt = cnt;
	w->word = cuckoo_malloc(sizeof(char *) * cnt);
	w->lens = cuckoo_malloc(sizeof(int) * cnt);
	w->hash = cuckoo_malloc(sizeof(int) * cnt);

	idx = 0;

	while (w->word_cnt < cnt) {
		char *p = filename[idx];

		if (p == NULL) {
			break;
		}

		fp = fopen(p, "r");

		if (fp == NULL) {
			perror("Error opening file");
			break;
		}

		while (w->word_cnt < cnt) {
			if (fgets(str, 512, fp) != NULL) {
				int len;

				w->word[w->word_cnt] = strdup(str);
				len = strlen(str);
				w->lens[w->word_cnt] = len;
				w->hash[w->word_cnt] = cuckoo_hash(str, len);

				//printf("%d: [%s], len=%d \n", w->word_cnt, str, w->lens[w->word_cnt]);

				w->word_cnt++;
			}
			else {
				break;
			}
		}

		fclose(fp);
		idx++;
	}

	return;
}

void free_word_list(word_list_t *w)
{
	int i;

	for (i = 0; i < w->word_cnt; i++) {
		cuckoo_free(w->word[i]);
	}

	cuckoo_free(w->word);
	cuckoo_free(w->lens);
	cuckoo_free(w->hash);
}

int _compare_key(const void *key1, const void *key2, const size_t nkey)
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

cuckoo_item_t* alloc_item(const char *key, const char *val, int len)
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

void free_item(cuckoo_item_t *it)
{
	if (it == NULL || it == RET_PTR_ERR) {
		return;
	}

	cuckoo_free((void*)it->key);
	cuckoo_free(it->value);
	cuckoo_free(it);
}

struct timespec diff_time(char *msg, struct timespec start, struct timespec end, int cnt)
{
	struct timespec temp;

	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}

	uint64_t nsec = temp.tv_sec * 1000000000 + temp.tv_nsec;
	uint64_t per_nsec = nsec / cnt;

	printf("%s Exec time: %lu.%lu sec, Count:%d, Per nsec:%lu \n", msg, 
			temp.tv_sec, temp.tv_nsec, cnt, per_nsec);

	return temp;
}

int test_lock_cuckoo(word_list_t *w, int thread_test)
{
	int i, inserted;
	char *key;
	char *val;
	uint32_t klen;
	cuckoo_hash_table_t *cukht;
	cuckoo_item_t **items;

	items = cuckoo_malloc(sizeof(cuckoo_item_t*) * w->word_cnt);

	for (i = 0; i < w->word_cnt; i++) {
		key = w->word[i];
		val = key;

		cuckoo_item_t* it = alloc_item(key, val, w->lens[i]);
		items[i] = it;
	}

	cukht = cuckoo_init_hash_table(23, _compare_key);

	printf("Hashtable size: %lu\n", cukht->hash_size);
	fflush(NULL);

	struct timespec begin, end;
	clockid_t cid;
	cid = CLOCK_MONOTONIC;
	//cid = CLOCK_PROCESS_CPUTIME_ID;
	//cid = CLOCK_THREAD_CPUTIME_ID;

	clock_gettime(cid, &begin);

	for (i = 0; i < w->word_cnt; i++) {
		cuckoo_item_t *it = items[i];

		int ret = cuckoo_insert(cukht, w->hash[i], it);
		if (ret == 0) {
			printf("insert failed: ret=%d, key=%s, hash=%u \n", ret, w->word[i], w->hash[i]);

			free_item(it);
			break;
		}
	}

	clock_gettime(cid, &end);
	diff_time("Insert", begin, end, w->word_cnt);

	printf("adding key pairs: %d of %d \n", cukht->num_items, w->word_cnt);
	printf("Cuckoo Movements: %d \n", cukht->num_moves);
	fflush(NULL);

	inserted = cukht->num_items;
	int found = 0;

	char *val1 = NULL;
	cuckoo_item_t *it;

	clock_gettime(cid, &begin);

	for (i = 0; i < inserted; i++) {
		key = w->word[i];
		klen = w->lens[i];
		val = key;

		//__builtin_prefetch(key);
		it = cuckoo_find(cukht, (const char *)key, (const size_t)klen, w->hash[i]);
		if (it) {
			val1 = (char *)it->value;
		}

		if (val1 == NULL || strncmp(val, val1, klen) != 0) {
			//printf("%d: mismatch value: val=%s, %s \n", i, val, val1);
		}
		else {
			found++;
		}
	}

	clock_gettime(cid, &end);
	diff_time("Search", begin, end, w->word_cnt);

	printf("Hashtable size: %lu\n", cukht->hash_size);
	printf("lookup key pairs: %d of %d \n", found, cukht->num_items);
	printf("done. \n");

	if (thread_test) {
		thread_main(w, cukht);
	}

	// cleaning 

	printf("Clean all items\n");

	cuckoo_free(items);

	for (i = 0; i < inserted; i++) {
		key = w->word[i];
		klen = w->lens[i];
		cuckoo_item_t *data;

		data = cuckoo_delete(cukht, key, klen, w->hash[i]);
		if (data != RET_PTR_ERR && data != NULL) {
			free_item(data);
		}
	}

	cuckoo_destroy_hash_table(cukht);

	printf("Clean wordlist\n");
	free_word_list(w);

	return 0;
}

start_routine main_list[] = {
	thread_main_reinsert,
	//thread_main_delete,
	thread_main_reinsert,
	thread_main_delete,
};

int thread_main(word_list_t *w, cuckoo_hash_table_t *cukht)
{
	int rc;
	int status;
	int i;

#define NUM_THREAD (int)(sizeof(main_list) / sizeof(start_routine))

	thread_arg_t t[NUM_THREAD];

	srand(time(NULL));

	for (i=0; i<NUM_THREAD; i++) {
		t[i].idx = i+1;
		t[i].w = w;
		t[i].cukht = cukht;
		//t[i].nwait = (rand() % 10) + 1;
		t[i].nwait = 1;
		t[i].round = 2;
	}

	for (i = 0; i < NUM_THREAD; i++) {
		done[i] = 0;
		rc = pthread_create(&threads[i], NULL, main_list[i], (void *)&t[i]);
		//printf("thread: rc=%d \n", rc);
	}

	for (i = 0; i < NUM_THREAD; i++) {
		done[i] = 1;

		rc = pthread_join(threads[i], (void **)&status);
		if (rc == 0) {
			//printf("Completed thread %d: status=%d\n", i+1, status);
		}
		else {
			printf("ERROR; ret=%d, thread %d\n", rc, i);
		}
	}

	return 0;
}

void *thread_main_reinsert(void *arg)
{
	thread_arg_t *t;
	pthread_t id = pthread_self();
	cuckoo_hash_table_t *cukht;
	word_list_t *w;

	t = (thread_arg_t*)arg;
	cukht = t->cukht;
	w = t->w;

	printf("##### Reinsert thread: running thread(%d: %lu), wait %d sec \n", 
		   t->idx, id, t->nwait);
	fflush(NULL);

	sleep(t->nwait);

	int i, j;
	char *key;
	char *val;
	uint32_t klen, hash;
	int reinserted_cnt=0;
	int try_cnt = 0, found=0;

	for (i=0; i<t->round; i++) {
		printf("%d: finding round=%d \n", t->idx, i);
		fflush(NULL);

		for (j = 0; j < w->word_cnt; j++) {
			key = w->word[j];
			val = key;
			klen = w->lens[j];
			hash = w->hash[j];

			cuckoo_item_t *it;

			try_cnt ++;
			it = cuckoo_find(cukht, key, klen, hash);
			if (it != NULL) {
				found ++;
				continue;
			}

			if ((it = alloc_item(key, val, klen)) != NULL) {
				int ret;
				pthread_spin_lock(&cukht->wlocks);
				ret = cuckoo_insert(cukht, hash, it);
				pthread_spin_unlock(&cukht->wlocks);
				if (ret) {
					reinserted_cnt ++;
				}
				else {
					free_item(it);
				}
			}
			else {
				printf("no more momory !!! \n");
			}
		}
	}

	printf("%d: Reinsert: Round=%d, try=%d, found=%d, reinserted=%d \n", 
		   t->idx, i, try_cnt, found, reinserted_cnt);
	fflush(NULL);

	pthread_exit((void *) 0);

	return NULL;
}

void *thread_main_delete(void *arg)
{
	thread_arg_t *t;
	pthread_t id = pthread_self();
	cuckoo_hash_table_t *cukht;
	word_list_t *w;

	t = (thread_arg_t*)arg;
	cukht = t->cukht;
	w = t->w;

	printf("##### Delete thread: running thread(%d: %lu), wait %d sec\n", 
		   t->idx, id, t->nwait);
	fflush(NULL);

	sleep(t->nwait);

	int i, j;
	char *key;
	uint32_t klen, hash;
	int deleted_cnt=0, not_deleted_cnt=0;
	int not_found=0;
	void *data;
	int try_cnt=0;

	for (i=0; i<t->round; i++) {
		for (j = 0; j < w->word_cnt; j++) {
			key = w->word[j];
			klen = w->lens[j];
			hash = w->hash[j];

			try_cnt ++;
			pthread_spin_lock(&cukht->wlocks);
			data = cuckoo_delete(cukht, key, klen, hash);
			pthread_spin_unlock(&cukht->wlocks);
			if (data == RET_PTR_ERR) {
				not_found ++;
			}
			else if (data) {
				free_item(data);
				deleted_cnt ++;
			}
			else {
				not_deleted_cnt ++;
			}
		}
	}

	printf("%d: Deleted : Round=%d, try=%d, deleted=%d, not_deleted=%d, not found=%d \n", 
		   t->idx, i, try_cnt, deleted_cnt, not_deleted_cnt, not_found);
	fflush(NULL);

	pthread_exit((void *) 0);

	return NULL;
}

int main()
{
	word_list_t w;

	memset(&w, 0, sizeof(w));

	printf("Start testing... \n");
	printf("pid=%d\n", getpid());
	fflush(NULL);

	//#define MAX_WORD  80000
	//#define MAX_WORD  2434783
	//#define MAX_WORD  9462148
//#define MAX_WORD    9462148
#define MAX_WORD    2

	get_words(&w, MAX_WORD);

	printf("word count:%d \n", w.word_cnt);
	fflush(NULL);

	test_lock_cuckoo(&w, 0);
	
	return 0;
}

/*
 * 10G bps
 * 64 Bytes packets: 14.88Mpps
 * 67.4 ns
 * cache-misses: 32ns
 * cache-reference:
 * L2: 4.3ns
 * L3: 7.9ns
 * lock: 8.25ns
 * systel call: 87.77ns
 */

