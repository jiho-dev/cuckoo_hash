#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <sched.h>   //cpu_set_t , CPU_SET

#include "cuckoo.h"
#include "cuckoo_malloc.h"

#include "benchmark.h"

pthread_t threads[5];
int done[5];


void *thread_main_delete(void *arg);
void *thread_main_reinsert(void *arg);
void *thread_main_search(void *arg);
int thread_main(word_list_t *w, cuckoo_bench_t *bm, void *ht);

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
	w->items = (void**)cuckoo_malloc(sizeof(void*) * cnt);

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
	cuckoo_free(w->items);
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

thread_function main_list[] = {
	thread_main_reinsert,
	thread_main_delete,
	thread_main_reinsert,
	thread_main_delete,
	thread_main_search,
};

int thread_main(word_list_t *w, cuckoo_bench_t *bm, void *ht)
{
	int rc;
	int status;
	int i;

#define NUM_THREAD (int)(sizeof(main_list) / sizeof(thread_function))

	thread_arg_t t[NUM_THREAD];

	srand(time(NULL));

	printf("Created Thread: %d \n", NUM_THREAD);

	for (i=0; i<NUM_THREAD; i++) {
		t[i].idx = i+1;
		t[i].w = w;
		t[i].hashtable = ht;
		//t[i].nwait = (rand() % 10) + 1;
		t[i].nwait = 1;
		t[i].round = 10;
		t[i].bm = bm;
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

int stick_thread(int idx)
{
	cpu_set_t cpuset; 

	int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
	if (idx < 0 || idx >= num_cores)
		return EINVAL;

	CPU_ZERO(&cpuset);       //clears the cpuset
	CPU_SET(idx, &cpuset); //set CPU 2 on cpuset

	/*
	 * cpu affinity for the calling thread 
	 * first parameter is the pid, 0 = calling thread
	 * second parameter is the size of your cpuset
	 * third param is the cpuset in which your thread will be
	 * placed. Each bit represents a CPU
	 */

	pthread_t current_thread = pthread_self();    
	sched_setaffinity(current_thread, sizeof(cpuset), &cpuset);

	return 0;
}

void *thread_main_reinsert(void *arg)
{
	thread_arg_t *t;
	pthread_t id = pthread_self();
	word_list_t *w;
	void *ht;
	cuckoo_bench_t *bm;

	t = (thread_arg_t*)arg;
	ht = t->hashtable;
	w = t->w;
	bm = t->bm;

	stick_thread(t->idx+1);

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
		printf("%d: Reinsert round=%d \n", t->idx, i);
		fflush(NULL);

		for (j = 0; j < w->word_cnt; j++) {
			key = w->word[j];
			val = key;
			klen = w->lens[j];
			hash = w->hash[j];

			try_cnt ++;

			// call fglock_bench_search_item()
			int ret = bm->bench_search_item(ht, (const char *)key, (const size_t)klen, hash);
			if (ret == 0) {
				found ++;
				continue;
			}

			// call fglock_bench_alloc_insert_data()
			if (bm->bench_alloc_insert_data(ht, key, klen, val, hash) == 0) {
				reinserted_cnt ++;
			}
		}
	}

	printf("%d: Reinsert: Round=%d, try=%d, found=%d, reinserted=%d \n", 
		   t->idx, i, try_cnt, found, reinserted_cnt);
	fflush(NULL);

	pthread_exit((void *) 0);

	return NULL;
}

void *thread_main_search(void *arg)
{
	thread_arg_t *t;
	pthread_t id = pthread_self();
	word_list_t *w;
	void *ht;
	cuckoo_bench_t *bm;

	t = (thread_arg_t*)arg;
	ht = t->hashtable;
	w = t->w;
	bm = t->bm;

	stick_thread(t->idx+1);

	printf("##### Search thread: running thread(%d: %lu), wait %d sec \n", 
		   t->idx, id, t->nwait);
	fflush(NULL);

	sleep(t->nwait);

	int i, j;
	char *key;
	uint32_t klen, hash;
	int try_cnt = 0, found=0;

	for (i=0; i<t->round; i++) {
		printf("%d: Searching round=%d \n", t->idx, i);
		fflush(NULL);

		for (j = 0; j < w->word_cnt; j++) {
			key = w->word[j];
			klen = w->lens[j];
			hash = w->hash[j];

			try_cnt ++;

			// call fglock_bench_search_item()
			int ret = bm->bench_search_item(ht, (const char *)key, (const size_t)klen, hash);
			if (ret == 0) {
				found ++;
				continue;
			}
		}
	}

	printf("%d: Searching: Round=%d, try=%d, found=%d \n", t->idx, i, try_cnt, found);
	fflush(NULL);

	pthread_exit((void *) 0);

	return NULL;
}

void *thread_main_delete(void *arg)
{
	thread_arg_t *t;
	pthread_t id = pthread_self();
	void *ht;
	word_list_t *w;
	cuckoo_bench_t *bm;

	t = (thread_arg_t*)arg;
	ht = t->hashtable;
	w = t->w;
	bm = t->bm;

	stick_thread(t->idx+1);

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
		printf("%d: Delete round=%d \n", t->idx, i);
		fflush(NULL);
		for (j = 0; j < w->word_cnt; j++) {
			key = w->word[j];
			klen = w->lens[j];
			hash = w->hash[j];

			try_cnt ++;
			
			data = bm->bench_delete_item(ht, key, klen, hash);
			if (data == RET_PTR_ERR) {
				not_found ++;
			}
			else if (data) {
				deleted_cnt ++;
			}
			else {
				not_deleted_cnt ++;
			}
		}
	}

	printf("%d: Delete : Round=%d, try=%d, deleted=%d, not_deleted=%d, not found=%d \n", 
		   t->idx, i, try_cnt, deleted_cnt, not_deleted_cnt, not_found);
	fflush(NULL);

	pthread_exit((void *) 0);

	return NULL;
}

/////////////////////////////////////////

int run_benchmark(word_list_t *w, cuckoo_bench_t *bm, int thread_test)
{
	int i, inserted;
	char *key;
	uint32_t klen;

	printf("===== Start Benchmark test: %s ===== \n", bm->name);
	fflush(NULL);

	// init the hash table and entries
	// call fglock_bench_init_hash_table()
	void *ht = bm->bench_init_hash_table(w);

	if (ht == NULL) {
		printf("Cannot init hashtable \n");
		return -1;
	}

	////////////////////////////////
	// inserting 

	struct timespec begin, end;
	clockid_t cid;
	cid = CLOCK_MONOTONIC;
	//cid = CLOCK_PROCESS_CPUTIME_ID;
	//cid = CLOCK_THREAD_CPUTIME_ID;

	clock_gettime(cid, &begin);

	for (i = 0; i < w->word_cnt; i++) {
		key = w->word[i];
		klen = w->lens[i];
		void *it = w->items[i];

		// call fglock_bench_insert_item()
		int ret = bm->bench_insert_item(ht, key, klen, NULL, w->hash[i], it);
		//printf("insert : ret=%d, key=%s, hash=%u \n", ret, w->word[i], w->hash[i]);
		if (ret == 0) {
			printf("insert failed: ret=%d, key=%s, hash=%u \n", ret, w->word[i], w->hash[i]);
			break;
		}
	}

	clock_gettime(cid, &end);
	diff_time("Insert", begin, end, w->word_cnt);

	// call fglock_bench_print_hash_table_info()
	bm->bench_print_hash_table_info(ht, "Adding");

	/////////////////////////////////////
	// searching

	inserted = i;
	int found = 0;

	clock_gettime(cid, &begin);

	for (i = 0; i < inserted; i++) {
		key = w->word[i];
		klen = w->lens[i];

		// call fglock_bench_search_item()
		int ret = bm->bench_search_item(ht, (const char *)key, (const size_t)klen, w->hash[i]);
		if (ret == 0) {
			found++;
		}
		else {
			//printf("%d: mismatch value: val=%s, %s \n", i, val, val1);
		}
	}

	clock_gettime(cid, &end);
	diff_time("Search", begin, end, w->word_cnt);

	//printf("Hashtable size: %lu\n", fglock_ht->hash_size);
	printf("lookup key pairs: %d of %d \n", found, inserted);
	printf("done. \n");

	///////////////////////////
	// threading

	if (thread_test) {
		thread_main(w, bm, ht);
	}

	//////////////////////////////////////
	// cleaning 

	printf("Clean all items\n");

	//cuckoo_free(items);

	for (i = 0; i < inserted; i++) {
		key = w->word[i];
		klen = w->lens[i];

		bm->bench_delete_item(ht, (const char *)key,  (const size_t)klen, w->hash[i]);
	}

	bm->bench_clean_hash_table(ht);

	printf("Clean wordlist\n");
	free_word_list(w);

	return 0;
}


/////////////////////////////////////////

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
#define MAX_WORD    9462148
//#define MAX_WORD    4000000

	get_words(&w, MAX_WORD);

	printf("word count:%d \n", w.word_cnt);
	fflush(NULL);

	run_benchmark(&w, &fglock_bench, 1);
	//run_benchmark(&w, &slot_bench, 0);
	
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

